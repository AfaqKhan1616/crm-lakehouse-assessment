# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_baseline_queries — reporting directly on the wide table
# MAGIC
# MAGIC **Baseline model:** no Bronze -> Silver -> Gold pipeline. Reports read the generated wide table
# MAGIC through `v_valid_events`, so the validity rules and deduplication run **inside every query**.
# MAGIC That is the cost the baseline pays; the pipeline pays it once per refresh instead.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./00_rules

# COMMAND ----------

# MAGIC %run ./00_queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — daily event counts by campaign and channel

# COMMAND ----------

q1 = spark.sql(QUERIES["Q1_wide"])
print(f"Q1 result rows: {q1.count():,}  (days x campaigns)")
display(q1.orderBy("event_date", "campaign_id").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — conversions and revenue by campaign over the selected date range

# COMMAND ----------

print(f"Date range: {Q2_START_DATE} .. {Q2_END_DATE}")
display(spark.sql(QUERIES["Q2_wide"]).orderBy("conversion_revenue", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall business totals (baseline)

# COMMAND ----------

display(spark.sql(QUERIES["TOTALS_wide"]))