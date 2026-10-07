# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04_gold — star schema (full refresh)
# MAGIC
# MAGIC | Table | Grain | Columns |
# MAGIC |---|---|---|
# MAGIC | `fact_marketing_events` | one valid event | event_id, contact_id, campaign_id, date_key, event_type, revenue |
# MAGIC | `dim_contact` | one contact | contact_id, contact_segment |
# MAGIC | `dim_campaign` | one campaign | campaign_id, campaign_name, channel |
# MAGIC | `dim_date` | one calendar day | date_key, calendar_date, month, year, quarter, day_name |
# MAGIC
# MAGIC - Source ids are the dimension keys (no surrogate keys; attributes never change, so current-state = SCD1 is enough).
# MAGIC - `date_key` = `yyyyMMdd` integer from the UTC event date.
# MAGIC - Dimensions come from **distinct valid Silver attributes**; measures stay in the fact table.
# MAGIC - `dim_date` covers every day from the first to the last event date, even days without events.
# MAGIC - Every table uses `CREATE OR REPLACE`, so a rerun rebuilds identical tables (no appends).
# MAGIC
# MAGIC **Gold load time** (sum of the four builds) is the "warehouse loading" measure for this exercise.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

builds = {
    T_DIM_CONTACT: f"""
        CREATE OR REPLACE TABLE {T_DIM_CONTACT} AS
        SELECT DISTINCT contact_id, contact_segment
        FROM {T_SILVER}""",

    T_DIM_CAMPAIGN: f"""
        CREATE OR REPLACE TABLE {T_DIM_CAMPAIGN} AS
        SELECT DISTINCT campaign_id, campaign_name, channel
        FROM {T_SILVER}""",

    T_DIM_DATE: f"""
        CREATE OR REPLACE TABLE {T_DIM_DATE} AS
        SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
               d                  AS calendar_date,
               month(d)           AS month,
               year(d)            AS year,
               quarter(d)         AS quarter,
               date_format(d, 'EEEE') AS day_name
        FROM (SELECT explode(sequence(min(event_date), max(event_date), INTERVAL 1 DAY)) AS d
              FROM {T_SILVER})""",

    T_FACT: f"""
        CREATE OR REPLACE TABLE {T_FACT} AS
        SELECT event_id,
               contact_id,
               campaign_id,
               CAST(date_format(event_date, 'yyyyMMdd') AS INT) AS date_key,
               event_type,
               revenue
        FROM {T_SILVER}""",
}

timings = {}
for table, sql in builds.items():
    timings[table] = run_sql_timed(sql)
    print(f"{table:50s} {timings[table]:>8}s")

gold_seconds = round(sum(timings.values()), 2)
print(f"\nGold load time (all four tables): {gold_seconds}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table sizes and row counts

# COMMAND ----------

rows = []
for table in [T_SOURCE, T_SILVER, T_FACT, T_DIM_CONTACT, T_DIM_CAMPAIGN, T_DIM_DATE]:
    size_bytes, num_files = table_size(table)
    n = spark.table(table).count()
    rows.append((table.split(".")[-1], n, round(size_bytes / 1024**2, 1), num_files))
    if table in timings:
        log_step(f"gold_build:{table.split('.')[-1]}", timings[table], n)
log_step("gold_build:total", gold_seconds)

display(spark.createDataFrame(rows, "table string, rows long, size_mb double, files int"))