# Databricks notebook source
# MAGIC %md
# MAGIC # 01_generate_source — synthetic marketing CRM events
# MAGIC
# MAGIC Builds **one wide Delta table**: one row per marketing interaction, with contact and
# MAGIC campaign attributes repeated on every row. This single table is used three ways:
# MAGIC 1. the **source** system (fictional CRM export)
# MAGIC 2. the **baseline** (reports query it directly)
# MAGIC 3. the **Bronze** layer (input to the Silver -> Gold pipeline)
# MAGIC
# MAGIC **Design choices**
# MAGIC - Rows come from `spark.range(N)`. Every column is computed from the row id with Spark
# MAGIC   expressions on the executors, so nothing is collected on the driver and it scales.
# MAGIC - Randomness uses `xxhash64(id, SEED, salt)`, not `rand()`. A hash of the id gives the
# MAGIC   same value no matter how Spark splits the work into partitions, so reruns produce
# MAGIC   **identical data**. (`rand(seed)` depends on partitioning, which serverless may change.)
# MAGIC - Contact and campaign attributes are derived from their own id, so each contact always
# MAGIC   has the same segment and each campaign the same name/channel. No history (SCD) logic needed.
# MAGIC - Campaign activity is **skewed**: a few campaigns get most events.
# MAGIC - A **known** number of duplicates and invalid rows are injected, so validation has
# MAGIC   exact expected results.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

# ---- Value lists (synthetic, fictional) ----
SEGMENTS = ["Small business", "Enterprise", "Consumer", "Mid-market"]
CHANNELS = ["Email", "Paid search", "Social", "Display", "Webinar"]
THEMES = ["Spring", "Summer", "Launch", "Retargeting", "Newsletter", "Webinar invite", "Holiday", "Loyalty"]

# Event mix (cumulative thresholds on a 0..1 random number)
#   impression 50% | open 20% | click 25% | conversion 5%
EVENT_TYPE_THRESHOLDS = [(0.50, "impression"), (0.70, "open"), (0.95, "click"), (1.00, "conversion")]
REVENUE_MIN, REVENUE_MAX = 10.0, 500.0          # only conversions carry revenue

# ---- Time range as epoch seconds (UTC), END_DATE inclusive ----
start_dt = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
end_dt = datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc) + timedelta(days=1)
START_EPOCH = int(start_dt.timestamp())
SPAN_SECONDS = int((end_dt - start_dt).total_seconds())

print(f"Generating {N_EVENTS:,} events | {N_CONTACTS:,} contacts | {N_CAMPAIGNS} campaigns "
      f"| {START_DATE}..{END_DATE} | seed={SEED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: deterministic pseudo-random numbers

# COMMAND ----------

def u01(col, salt):
    """Deterministic 'random' number in [0, 1) from a column + SEED + salt.
    A different salt gives an independent number for each attribute."""
    return F.pmod(F.xxhash64(col, F.lit(SEED), F.lit(salt)), F.lit(1_000_000_000)) / F.lit(1_000_000_000.0)

def pick(values, index_col):
    """Choose values[index] from a Python list inside Spark (index is 0-based)."""
    return F.element_at(F.array(*[F.lit(v) for v in values]), (index_col + 1).cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build clean events

# COMMAND ----------

def build_events(ids_df):
    """ids_df has one column `id`. Returns the wide event schema, all rows valid."""
    df = ids_df

    # Contact: uniform over N_CONTACTS
    df = df.withColumn("contact_idx", F.pmod(F.xxhash64("id", F.lit(SEED), F.lit("contact")), F.lit(N_CONTACTS)))

    # Campaign: skewed. u^3 pushes values toward 0, so low campaign numbers get most events.
    df = df.withColumn("campaign_idx", F.floor(F.pow(u01(F.col("id"), "campaign"), 3) * N_CAMPAIGNS).cast("long"))

    # Event type from cumulative thresholds
    r_type = u01(F.col("id"), "event_type")
    event_type = F.lit(EVENT_TYPE_THRESHOLDS[-1][1])
    for threshold, name in reversed(EVENT_TYPE_THRESHOLDS[:-1]):
        event_type = F.when(r_type < threshold, F.lit(name)).otherwise(event_type)

    df = (df
        # Identifiers (zero-padded strings like the example E001 / C101 / M01)
        .withColumn("event_id", F.concat(F.lit("E"), F.lpad(F.col("id").cast("string"), 12, "0")))
        .withColumn("contact_id", F.concat(F.lit("C"), F.lpad((F.col("contact_idx") + 1).cast("string"), 9, "0")))
        .withColumn("campaign_id", F.concat(F.lit("M"), F.lpad((F.col("campaign_idx") + 1).cast("string"), 4, "0")))

        # Contact attribute: derived from the contact only -> consistent for every event
        .withColumn("contact_segment",
                    pick(SEGMENTS, F.floor(u01(F.col("contact_idx"), "segment") * len(SEGMENTS))))

        # Campaign attributes: derived from the campaign only -> consistent for every event
        .withColumn("channel", pick(CHANNELS, F.pmod(F.col("campaign_idx"), F.lit(len(CHANNELS)))))
        .withColumn("campaign_name",
                    F.concat_ws(" ",
                                pick(THEMES, F.pmod(F.col("campaign_idx"), F.lit(len(THEMES)))),
                                F.lower(F.col("channel")),
                                F.concat(F.lit("#"), (F.col("campaign_idx") + 1).cast("string"))))

        # Event fields
        .withColumn("event_type", event_type)
        .withColumn("event_timestamp",
                    F.timestamp_seconds(F.lit(START_EPOCH) +
                                        F.floor(u01(F.col("id"), "ts") * SPAN_SECONDS).cast("long")))
        .withColumn("revenue",
                    F.when(F.col("event_type") == "conversion",
                           F.round(F.lit(REVENUE_MIN) + u01(F.col("id"), "revenue") * (REVENUE_MAX - REVENUE_MIN), 2))
                     .otherwise(F.lit(0))
                     .cast("decimal(12,2)"))
    )
    return df

SOURCE_COLUMNS = ["event_id", "contact_id", "contact_segment", "campaign_id", "campaign_name",
                  "channel", "event_type", "event_timestamp", "revenue"]

clean = build_events(spark.range(N_EVENTS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inject known bad rows
# MAGIC | Kind | Count | How | Expected pipeline action |
# MAGIC |---|---|---|---|
# MAGIC | Exact duplicates | `N_DUPLICATES` | copies of evenly spaced clean events | removed by dedup |
# MAGIC | Invalid rows | `N_INVALID` | new event_ids, each breaking one rule (4 kinds, round-robin) | rejected |

# COMMAND ----------

# Duplicates: every `step`-th clean event appears twice. Exact copies, like E003 in the brief.
dup_step = max(N_EVENTS // N_DUPLICATES, 1)
duplicates = clean.filter((F.col("id") % dup_step == 0) & (F.col("id") < dup_step * N_DUPLICATES))

# Invalid rows: ids after the clean range, so their event_ids are unique.
invalid = (build_events(spark.range(N_EVENTS, N_EVENTS + N_INVALID))
    .withColumn("kind", F.pmod(F.col("id"), F.lit(4)))
    .withColumn("contact_id",      F.when(F.col("kind") == 0, F.lit(None).cast("string")).otherwise(F.col("contact_id")))
    .withColumn("campaign_id",     F.when(F.col("kind") == 1, F.lit(None).cast("string")).otherwise(F.col("campaign_id")))
    .withColumn("event_timestamp", F.when(F.col("kind") == 2, F.lit(None).cast("timestamp")).otherwise(F.col("event_timestamp")))
    .withColumn("event_type",      F.when(F.col("kind") == 3, F.lit("unknown")).otherwise(F.col("event_type")))
)

source = (clean.select(SOURCE_COLUMNS)
          .unionByName(duplicates.select(SOURCE_COLUMNS))
          .unionByName(invalid.select(SOURCE_COLUMNS)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the wide Delta table (full overwrite = rerun-safe)

# COMMAND ----------

t0 = time.time()
(source.write
       .mode("overwrite")                 # rerun replaces, never appends
       .option("overwriteSchema", "true")
       .saveAsTable(T_SOURCE))
write_seconds = round(time.time() - t0, 1)
print(f"Wrote {T_SOURCE} in {write_seconds}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure size (active files only, from the Delta log)

# COMMAND ----------

detail = spark.sql(f"DESCRIBE DETAIL {T_SOURCE}").select("sizeInBytes", "numFiles").first()
total_rows = spark.table(T_SOURCE).count()

size_bytes = detail["sizeInBytes"]
bytes_per_row = size_bytes / total_rows
target_bytes = 30 * 1024**3
rows_for_30gb = int(target_bytes / bytes_per_row)

print(f"Rows            : {total_rows:,}")
print(f"Active size     : {size_bytes / 1024**2:,.1f} MB  ({size_bytes:,} bytes)")
print(f"Files           : {detail['numFiles']}")
print(f"Bytes per row   : {bytes_per_row:.1f}")
print(f"N_EVENTS for 30 GB (estimate): ~{rows_for_30gb:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record expected validation results and the run log

# COMMAND ----------

expected = {
    "run_label": RUN_LABEL,
    "seed": SEED,
    "n_events": N_EVENTS,
    "n_contacts": N_CONTACTS,
    "n_campaigns": N_CAMPAIGNS,
    "start_date": START_DATE,
    "end_date": END_DATE,
    "expected_source_rows": N_EVENTS + N_DUPLICATES + N_INVALID,
    "expected_duplicates_removed": N_DUPLICATES,
    "expected_rejected_rows": N_INVALID,
    "expected_valid_events": N_EVENTS,
}
(spark.createDataFrame([expected]).write.mode("overwrite")
      .option("overwriteSchema", "true").saveAsTable(T_GEN_PARAMS))

run_log = dict(expected, total_rows=total_rows, size_bytes=size_bytes,
               num_files=detail["numFiles"], write_seconds=write_seconds,
               captured_at_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"))
spark.createDataFrame([run_log]).write.mode("append").option("mergeSchema", "true").saveAsTable(T_GEN_LOG)

assert total_rows == expected["expected_source_rows"], "Row count does not match config"
print("Expected results saved. Row count matches config: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks (look, don't fix: the pipeline handles bad rows)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  count(*)                                              AS total_rows,
  count(DISTINCT event_id)                              AS distinct_event_ids,
  count(*) - count(DISTINCT event_id)                   AS duplicate_rows,
  sum(CASE WHEN contact_id IS NULL THEN 1 ELSE 0 END)      AS null_contact,
  sum(CASE WHEN campaign_id IS NULL THEN 1 ELSE 0 END)     AS null_campaign,
  sum(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
  sum(CASE WHEN event_type NOT IN ('impression','open','click','conversion') THEN 1 ELSE 0 END) AS bad_event_type,
  min(event_timestamp)                                  AS first_event,
  max(event_timestamp)                                  AS last_event
FROM {T_SOURCE}
"""))

# COMMAND ----------

# Event mix and revenue rule: only conversions should have revenue > 0
display(spark.sql(f"""
SELECT event_type,
       count(*)                          AS events,
       round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct,
       sum(revenue)                      AS revenue,
       sum(CASE WHEN revenue > 0 THEN 1 ELSE 0 END) AS rows_with_revenue
FROM {T_SOURCE}
GROUP BY event_type
ORDER BY events DESC
"""))

# COMMAND ----------

# Skew: share of events in the top campaigns
display(spark.sql(f"""
SELECT campaign_id, campaign_name, channel,
       count(*) AS events,
       round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct_of_all
FROM {T_SOURCE}
WHERE campaign_id IS NOT NULL
GROUP BY campaign_id, campaign_name, channel
ORDER BY events DESC
LIMIT 10
"""))

# COMMAND ----------

# Consistency: every contact has exactly one segment, every campaign one name/channel.
# Both numbers must be 0 (this is why no dimension history logic is needed).
display(spark.sql(f"""
SELECT
  (SELECT count(*) FROM (SELECT contact_id FROM {T_SOURCE} WHERE contact_id IS NOT NULL
                         GROUP BY contact_id HAVING count(DISTINCT contact_segment) > 1)) AS contacts_with_2plus_segments,
  (SELECT count(*) FROM (SELECT campaign_id FROM {T_SOURCE} WHERE campaign_id IS NOT NULL
                         GROUP BY campaign_id HAVING count(DISTINCT campaign_name, channel) > 1)) AS campaigns_with_2plus_attrs
"""))

# COMMAND ----------

display(spark.table(T_SOURCE).limit(10))
