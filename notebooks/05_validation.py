# Databricks notebook source
# MAGIC %md
# MAGIC # 05_validation — three checks + full-refresh rerun
# MAGIC
# MAGIC | # | Check | Pass condition |
# MAGIC |---|---|---|
# MAGIC | 0 | Rules on the brief's example rows | 3 valid events, 1 conversion, 120.00 revenue |
# MAGIC | 1 | **Event uniqueness** | fact: rows = distinct event_id; each dimension: rows = distinct key |
# MAGIC | 2 | **Required fields + dimension relationships** | no NULL fact keys/measures, no Silver rule violations, 0 orphan keys |
# MAGIC | 2b | Known bad rows handled | rejected = injected invalid, removed = injected duplicates |
# MAGIC | 3 | **Same business results in both models** | Q1, Q2 and totals: 0 differing rows in either direction |
# MAGIC | R | **Full-refresh rerun** | rerunning Silver + Gold gives identical row counts, totals and content checksums |
# MAGIC
# MAGIC Results are saved to `crm_meta.validation_results`. The last cell fails loudly if any check fails.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./00_rules

# COMMAND ----------

# MAGIC %run ./00_queries

# COMMAND ----------

from decimal import Decimal
from pyspark.sql import functions as F

results = []   # (check, detail, expected, actual)

def record(check, detail, expected, actual):
    results.append((check, detail, str(expected), str(actual), "PASS" if expected == actual else "FAIL"))

def scalar(sql):
    return spark.sql(sql).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 0 — the rules on the brief's example rows
# MAGIC Same `checked_sql` / `valid_sql` functions as the real pipeline, applied to the 5 rows from the brief.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW brief_example AS
SELECT * FROM VALUES
  ('E001', 'C101', 'Small business', 'M01', 'Spring email',       'Email',       'click',      TIMESTAMP'2026-03-01 09:00:00', CAST(0.00   AS DECIMAL(12,2))),
  ('E002', 'C101', 'Small business', 'M01', 'Spring email',       'Email',       'conversion', TIMESTAMP'2026-03-01 09:15:00', CAST(120.00 AS DECIMAL(12,2))),
  ('E003', 'C102', 'Enterprise',     'M02', 'Search promotion',   'Paid search', 'click',      TIMESTAMP'2026-03-02 10:00:00', CAST(0.00   AS DECIMAL(12,2))),
  ('E003', 'C102', 'Enterprise',     'M02', 'Search promotion',   'Paid search', 'click',      TIMESTAMP'2026-03-02 10:00:00', CAST(0.00   AS DECIMAL(12,2))),
  ('E004', NULL,   'Consumer',       'M01', 'Spring email',       'Email',       'click',      TIMESTAMP'2026-03-02 11:00:00', CAST(0.00   AS DECIMAL(12,2)))
AS t(event_id, contact_id, contact_segment, campaign_id, campaign_name, channel, event_type, event_timestamp, revenue)
""")
spark.sql(f"CREATE OR REPLACE TEMP VIEW brief_checked AS {checked_sql('brief_example')}")
spark.sql(f"CREATE OR REPLACE TEMP VIEW brief_valid AS {valid_sql('brief_checked')}")

ex = spark.sql("""
SELECT COUNT(*) AS events,
       SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) AS conversions,
       SUM(CASE WHEN event_type = 'conversion' THEN revenue ELSE 0 END) AS revenue
FROM brief_valid""").first()
record("0 brief example", "valid events", 3, ex["events"])
record("0 brief example", "conversions", 1, ex["conversions"])
record("0 brief example", "conversion revenue", Decimal("120.00"), ex["revenue"])
display(spark.sql("SELECT event_id, contact_id, rejection_reason FROM brief_checked ORDER BY event_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1 — event uniqueness (and dimension key uniqueness)

# COMMAND ----------

for table, key in [(T_FACT, "event_id"), (T_DIM_CONTACT, "contact_id"),
                   (T_DIM_CAMPAIGN, "campaign_id"), (T_DIM_DATE, "date_key")]:
    r = spark.sql(f"SELECT COUNT(*) AS n, COUNT(DISTINCT {key}) AS d FROM {table}").first()
    record("1 uniqueness", f"{table.split('.')[-1]}: rows - distinct {key}", 0, r["n"] - r["d"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2 — required fields and dimension relationships

# COMMAND ----------

# Required fields: fact keys and measures are never NULL
null_fact = scalar(f"""
    SELECT COUNT(*) FROM {T_FACT}
    WHERE event_id IS NULL OR contact_id IS NULL OR campaign_id IS NULL
       OR date_key IS NULL OR event_type IS NULL OR revenue IS NULL""")
record("2 required fields", "fact rows with a NULL key or measure", 0, null_fact)

# Silver obeys every rule: re-apply R1-R3 to Silver, nothing may be rejected
silver_violations = scalar(f"SELECT COUNT(*) FROM ({checked_sql(T_SILVER)}) WHERE rejection_reason IS NOT NULL")
record("2 required fields", "silver rows breaking R1-R3", 0, silver_violations)

# Relationships: every fact key exists in its dimension (LEFT ANTI JOIN = keys with no match)
for dim, key in [(T_DIM_CONTACT, "contact_id"), (T_DIM_CAMPAIGN, "campaign_id"), (T_DIM_DATE, "date_key")]:
    orphans = scalar(f"SELECT COUNT(*) FROM {T_FACT} f LEFT ANTI JOIN {dim} d ON f.{key} = d.{key}")
    record("2 relationships", f"fact.{key} missing in {dim.split('.')[-1]}", 0, orphans)

# Known bad rows: exactly the injected counts were removed
expected = spark.table(T_GEN_PARAMS).first()
source_rows = spark.table(T_SOURCE).count()
rejected_rows = scalar(f"SELECT COUNT(*) FROM {V_CHECKED} WHERE rejection_reason IS NOT NULL")
fact_rows = spark.table(T_FACT).count()
record("2b known bad rows", "rejected (invalid) rows", expected["expected_rejected_rows"], rejected_rows)
record("2b known bad rows", "duplicate rows removed", expected["expected_duplicates_removed"],
       source_rows - rejected_rows - fact_rows)
record("2b known bad rows", "valid events in fact", expected["expected_valid_events"], fact_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3 — same business results in both models
# MAGIC `EXCEPT ALL` in both directions: 0 and 0 means the two result sets are identical, row for row.

# COMMAND ----------

for q in ["Q1", "Q2", "TOTALS"]:
    wide, star = spark.sql(QUERIES[f"{q}_wide"]), spark.sql(QUERIES[f"{q}_star"])
    record("3 equivalence", f"{q}: result rows wide vs star", wide.count(), star.count())
    record("3 equivalence", f"{q}: rows only in wide", 0, wide.exceptAll(star).count())
    record("3 equivalence", f"{q}: rows only in star", 0, star.exceptAll(wide).count())

totals = (spark.sql(QUERIES["TOTALS_wide"]).withColumn("model", F.lit("wide (baseline)"))
          .unionByName(spark.sql(QUERIES["TOTALS_star"]).withColumn("model", F.lit("star (pipeline)"))))
display(totals.select("model", "events", "conversions", "conversion_revenue"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rerun — full refresh must reproduce the same facts and totals
# MAGIC Fingerprint = row count, revenue total and an order-independent content checksum per table.
# MAGIC Then Silver and Gold are rebuilt and fingerprinted again.

# COMMAND ----------

PIPELINE_TABLES = [T_SILVER, T_FACT, T_DIM_CONTACT, T_DIM_CAMPAIGN, T_DIM_DATE]

def fingerprint(table):
    df = spark.table(table)
    row_hash = F.pmod(F.xxhash64(*df.columns), F.lit(1_000_003)).cast("long")
    agg = [F.count("*").alias("rows"), F.sum(row_hash).alias("checksum")]
    if "revenue" in df.columns:
        agg.append(F.sum("revenue").alias("revenue"))
    r = df.agg(*agg).first()
    return (r["rows"], r["checksum"], r["revenue"] if "revenue" in df.columns else None)

def latest_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]

before = {t: fingerprint(t) for t in PIPELINE_TABLES}
version_before = {t: latest_version(t) for t in PIPELINE_TABLES}

# COMMAND ----------

# MAGIC %run ./03_silver

# COMMAND ----------

# MAGIC %run ./04_gold

# COMMAND ----------

after = {t: fingerprint(t) for t in PIPELINE_TABLES}
version_after = {t: latest_version(t) for t in PIPELINE_TABLES}

for t in PIPELINE_TABLES:
    name = t.split(".")[-1]
    record("R rerun", f"{name}: new Delta version written", True, version_after[t] > version_before[t])
    record("R rerun", f"{name}: rows",     before[t][0], after[t][0])
    record("R rerun", f"{name}: checksum", before[t][1], after[t][1])
    if before[t][2] is not None:
        record("R rerun", f"{name}: revenue", before[t][2], after[t][2])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

results_df = spark.createDataFrame(results, "check string, detail string, expected string, actual string, status string")
(results_df.withColumn("run_label", F.lit(RUN_LABEL))
           .withColumn("captured_at_utc", F.lit(datetime.now(timezone.utc).isoformat(timespec="seconds")))
           .write.mode("overwrite").option("overwriteSchema", "true")
           .saveAsTable(fq(SCHEMA_META, "validation_results")))
display(results_df)

failed = [r for r in results if r[4] == "FAIL"]
print(f"{len(results) - len(failed)} PASS, {len(failed)} FAIL")
assert not failed, f"Validation failed: {failed}"
