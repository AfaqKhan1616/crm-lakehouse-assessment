# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03_silver — validate and deduplicate (full refresh)
# MAGIC
# MAGIC - **Silver** = `v_valid_events` materialized: valid rows, one per `event_id`, plus `event_date`.
# MAGIC - **Quarantine** = rejected rows with the rule they broke (optional in the brief; kept for traceability).
# MAGIC - `CREATE OR REPLACE TABLE` makes every run a full refresh: rerunning gives the same table, never appends.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./00_rules

# COMMAND ----------

t_silver = run_sql_timed(f"CREATE OR REPLACE TABLE {T_SILVER} AS SELECT * FROM {V_VALID}")
t_quarantine = run_sql_timed(f"""
    CREATE OR REPLACE TABLE {T_QUARANTINE} AS
    SELECT * FROM {V_CHECKED} WHERE rejection_reason IS NOT NULL""")
print(f"Silver built in {t_silver}s, quarantine in {t_quarantine}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row accounting: source = silver + rejected + duplicates removed

# COMMAND ----------

source_rows = spark.table(T_SOURCE).count()
silver_rows = spark.table(T_SILVER).count()
rejected_rows = spark.table(T_QUARANTINE).count()
duplicates_removed = source_rows - rejected_rows - silver_rows

expected = spark.table(T_GEN_PARAMS).first()
checks = [
    ("source rows",        source_rows,        expected["expected_source_rows"]),
    ("rejected rows",      rejected_rows,      expected["expected_rejected_rows"]),
    ("duplicates removed", duplicates_removed, expected["expected_duplicates_removed"]),
    ("silver rows",        silver_rows,        expected["expected_valid_events"]),
]
for name, actual, exp in checks:
    print(f"{name:20s} actual={actual:>14,}  expected={exp:>14,}  {'PASS' if actual == exp else 'FAIL'}")

log_step("silver_build", t_silver, silver_rows)
log_step("quarantine_build", t_quarantine, rejected_rows,
         f"duplicates_removed={duplicates_removed}")

# COMMAND ----------

# Why rows were rejected
display(spark.sql(f"""
SELECT rejection_reason, COUNT(*) AS rows
FROM {T_QUARANTINE}
GROUP BY rejection_reason
ORDER BY rejection_reason"""))