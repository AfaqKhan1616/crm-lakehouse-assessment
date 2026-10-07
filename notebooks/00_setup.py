# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_setup — create schemas and record the environment
# MAGIC Run once after linking the repo. Safe to rerun (all statements are idempotent).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Create one schema per layer inside the Free Edition default catalog.
for s in [SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD, SCHEMA_META]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# Capture runtime details for the README. Each lookup is wrapped because
# some cluster-level configs do not exist on serverless compute.
import sys, platform, datetime

def safe(fn, default="not available on serverless"):
    try:
        return str(fn())
    except Exception:
        return default

env = {
    "captured_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds"),
    "spark_version": spark.version,
    "databricks_version": safe(lambda: spark.sql("SELECT current_version()").first()[0]),
    "python_version": sys.version.split()[0],
    "os": platform.platform(),
    "compute": "Free Edition serverless (size not configurable)",
    "default_parallelism": safe(lambda: spark.sparkContext.defaultParallelism),
    "shuffle_partitions": safe(lambda: spark.conf.get("spark.sql.shuffle.partitions")),
    "photon": safe(lambda: spark.conf.get("spark.databricks.photon.enabled")),
}

for k, v in env.items():
    print(f"{k:22s} {v}")

# COMMAND ----------

# Persist so the README numbers trace back to a table, not memory.
(spark.createDataFrame([env])
      .write.mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(T_ENV))

display(spark.table(T_ENV))