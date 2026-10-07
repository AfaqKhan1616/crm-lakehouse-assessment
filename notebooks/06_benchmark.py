# Databricks notebook source
# MAGIC %md
# MAGIC # 06_benchmark — compare both models, then one optimization
# MAGIC
# MAGIC **Queries** (from `00_queries`): Q1 daily events by campaign & channel, Q2 conversions & revenue by campaign for March.
# MAGIC
# MAGIC **Configurations**
# MAGIC | Config | Model | What it reads |
# MAGIC |---|---|---|
# MAGIC | `baseline` | wide | `v_valid_events`: wide table + rules + dedup **at query time** |
# MAGIC | `star` | star | Gold fact (as built by 04_gold, unclustered) + small dimensions |
# MAGIC | `star_clustered` | star | copy of the fact with **liquid clustering on `date_key`** |
# MAGIC
# MAGIC **Optimization chosen: liquid clustering of the fact table by `date_key`.**
# MAGIC Observed problem: the generator writes events in random date order, so every fact file spans the whole
# MAGIC half-year and a one-month filter (Q2) cannot skip any file. Clustering co-locates rows by date so each file
# MAGIC covers a narrow date range and Delta can skip files using their min/max statistics.
# MAGIC The clustered fact is a **separate copy** so before and after can be measured side by side without
# MAGIC changing the pipeline table.
# MAGIC
# MAGIC **Method:** each query runs twice per configuration (run 1 and run 2, to show cache effects).
# MAGIC Wall-clock time of `collect()`; results are small (at most days x campaigns rows).
# MAGIC Every statement carries a `/* bench:... */` tag so it can be found in **Query History** -> query profile.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./00_rules

# COMMAND ----------

# MAGIC %run ./00_queries

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

T_FACT_CLUSTERED = fq(SCHEMA_GOLD, "fact_marketing_events_clustered")
T_BENCH = fq(SCHEMA_META, "benchmark_results")
BENCH_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]

# Try to switch the disk cache off for fairer timings. Serverless may not allow it: record what happened.
# (The SQL-warehouse result cache does not apply to notebook queries; run 1 vs run 2 shows any remaining caching.)
try:
    spark.conf.set("spark.databricks.io.cache.enabled", "false")
    cache_note = "disk cache: disabled via spark.databricks.io.cache.enabled=false"
except Exception as e:
    cache_note = f"disk cache: not configurable on this compute ({type(e).__name__}); managed by serverless"
print("Benchmark run:", BENCH_RUN_ID)
print(cache_note)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the optimized copy (and measure what it costs)

# COMMAND ----------

t_ctas = run_sql_timed(f"""
    CREATE OR REPLACE TABLE {T_FACT_CLUSTERED}
    CLUSTER BY (date_key)
    AS SELECT * FROM {T_FACT}""")

# OPTIMIZE FULL reclusters every row (DBR 16+); plain OPTIMIZE as a fallback.
try:
    t_optimize = run_sql_timed(f"OPTIMIZE {T_FACT_CLUSTERED} FULL")
    optimize_mode = "OPTIMIZE FULL"
except Exception:
    t_optimize = run_sql_timed(f"OPTIMIZE {T_FACT_CLUSTERED}")
    optimize_mode = "OPTIMIZE"

log_step("bench:fact_clustered_ctas", t_ctas)
log_step("bench:fact_clustered_optimize", t_optimize, notes=optimize_mode)
print(f"Clustered copy: CTAS {t_ctas}s + {optimize_mode} {t_optimize}s = {round(t_ctas + t_optimize, 2)}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## File layout: how many files does a March query have to open?
# MAGIC For every data file, the min/max `date_key` it contains (`_metadata.file_path` = the file a row lives in).
# MAGIC A file must be read if its date range overlaps the query range; otherwise Delta can skip it.

# COMMAND ----------

lo, hi = date_key(Q2_START_DATE), date_key(Q2_END_DATE)

def layout(table):
    files = spark.sql(f"""
        SELECT _metadata.file_path AS file, MIN(date_key) AS min_key, MAX(date_key) AS max_key, COUNT(*) AS rows
        FROM {table} GROUP BY _metadata.file_path""")
    r = files.agg(
        F.count("*").alias("files"),
        F.sum(F.when((F.col("max_key") >= lo) & (F.col("min_key") <= hi), 1).otherwise(0)).alias("files_overlapping_q2"),
        F.sum(F.when((F.col("max_key") >= lo) & (F.col("min_key") <= hi), F.col("rows")).otherwise(0)).alias("rows_in_those_files"),
        F.round(F.avg(F.datediff(F.to_date(F.col("max_key").cast("string"), "yyyyMMdd"),
                                 F.to_date(F.col("min_key").cast("string"), "yyyyMMdd")) + 1), 1).alias("avg_days_per_file"),
    ).first()
    size_bytes, _ = table_size(table)
    return dict(table=table.split(".")[-1], size_mb=round(size_bytes / 1024**2, 1), **r.asDict())

layouts = [layout(T_FACT), layout(T_FACT_CLUSTERED)]
display(spark.createDataFrame(layouts))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the benchmark

# COMMAND ----------

def clustered(sql):
    return sql.replace(T_FACT, T_FACT_CLUSTERED)

plan = [
    ("Q1", "baseline",       QUERIES["Q1_wide"]),
    ("Q1", "star",           QUERIES["Q1_star"]),
    ("Q1", "star_clustered", clustered(QUERIES["Q1_star"])),
    ("Q2", "baseline",       QUERIES["Q2_wide"]),
    ("Q2", "star",           QUERIES["Q2_star"]),
    ("Q2", "star_clustered", clustered(QUERIES["Q2_star"])),
]

bench_rows = []
for query, config, sql in plan:
    for run in (1, 2):
        tag = f"/* bench:{BENCH_RUN_ID}:{query}:{config}:run{run} */"
        t0 = time.time()
        n = len(spark.sql(f"{tag} {sql}").collect())
        seconds = round(time.time() - t0, 2)
        bench_rows.append((BENCH_RUN_ID, RUN_LABEL, query, config, run, seconds, n))
        print(f"{query} {config:15s} run{run}: {seconds:>7}s  ({n} rows)")

bench_df = spark.createDataFrame(
    bench_rows, "bench_run_id string, run_label string, query string, config string, run int, seconds double, result_rows long")
bench_df.write.mode("append").saveAsTable(T_BENCH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary table (seconds)

# COMMAND ----------

display(bench_df.groupBy("query", "config")
        .pivot("run", [1, 2]).agg(F.first("seconds"))
        .withColumnRenamed("1", "run1_s").withColumnRenamed("2", "run2_s")
        .orderBy("query", F.expr("CASE config WHEN 'baseline' THEN 1 WHEN 'star' THEN 2 ELSE 3 END")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution metrics from Query History (if this account exposes system tables)
# MAGIC Bytes read, files read and files pruned per benchmark statement. If this cell fails, open
# MAGIC **SQL > Query History**, search for `bench:` and screenshot the query profile of Q2 star vs star_clustered.

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT regexp_extract(statement_text, 'bench:[^:]+:([^:]+):([^:]+):(run[12])', 1) AS query,
               regexp_extract(statement_text, 'bench:[^:]+:([^:]+):([^:]+):(run[12])', 2) AS config,
               regexp_extract(statement_text, 'bench:[^:]+:([^:]+):([^:]+):(run[12])', 3) AS run,
               total_duration_ms, read_bytes, read_files, pruned_files, shuffle_read_bytes
        FROM system.query.history
        WHERE statement_text LIKE '%bench:{BENCH_RUN_ID}%'
        ORDER BY query, config, run"""))
except Exception as e:
    print(f"system.query.history not available here ({type(e).__name__}). Use Query History UI instead.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query plans: Q2 on the star schema
# MAGIC Look for the `date_key` filter pushed into the fact scan (`PushedFilters` / data filters).
# MAGIC The plan is the same before and after clustering; the difference is how many files survive skipping at runtime.

# COMMAND ----------

print(spark.sql(f"EXPLAIN FORMATTED {clustered(QUERIES['Q2_star'])}").first()[0][:4000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load cost vs query gain
# MAGIC Gold load time comes from the pipeline log (04_gold). The clustered copy adds CTAS + OPTIMIZE.

# COMMAND ----------

display(spark.sql(f"""
SELECT step, round(seconds, 2) AS seconds, row_count, notes, captured_at_utc
FROM {T_PIPELINE_LOG}
WHERE run_label = '{RUN_LABEL}'
  AND (step LIKE 'silver%' OR step LIKE 'quarantine%' OR step LIKE 'gold_build%' OR step LIKE 'bench:%')
ORDER BY captured_at_utc DESC
LIMIT 20"""))
