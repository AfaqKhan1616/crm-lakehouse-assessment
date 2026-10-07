# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_rules — validity rules, defined once
# MAGIC
# MAGIC Both reporting approaches use **these two views**, so they apply identical rules:
# MAGIC - the **baseline** queries `v_valid_events` directly (rules applied at query time, every time)
# MAGIC - the **pipeline** materializes `v_valid_events` into Silver (rules applied once per refresh)
# MAGIC
# MAGIC | Rule | Definition | Action |
# MAGIC |---|---|---|
# MAGIC | R1 Required | event_id, contact_id, campaign_id, event_timestamp, event_type, revenue are NOT NULL | reject |
# MAGIC | R2 Allowed type | event_type in impression / open / click / conversion | reject |
# MAGIC | R3 Revenue | revenue >= 0, and revenue = 0 unless event_type = conversion | reject |
# MAGIC | R4 Unique | one row per event_id, applied **after** R1-R3; tie-break by timestamp, contact, campaign, type, revenue | drop extras |
# MAGIC
# MAGIC Order matters: rejecting first, then deduplicating, means a valid copy of an event always survives
# MAGIC even if another copy of the same event_id is invalid.
# MAGIC
# MAGIC Used via `%run ./00_rules` after `%run ./00_config`. Creating the views is idempotent and instant.

# COMMAND ----------

allowed_types = ", ".join(f"'{t}'" for t in VALID_EVENT_TYPES)

# Every source row, plus the first rule it breaks (NULL = valid).
spark.sql(f"""
CREATE OR REPLACE VIEW {V_CHECKED} AS
SELECT
  *,
  CASE
    WHEN event_id        IS NULL THEN 'R1_missing_event_id'
    WHEN contact_id      IS NULL THEN 'R1_missing_contact_id'
    WHEN campaign_id     IS NULL THEN 'R1_missing_campaign_id'
    WHEN event_timestamp IS NULL THEN 'R1_missing_event_timestamp'
    WHEN event_type      IS NULL THEN 'R1_missing_event_type'
    WHEN revenue         IS NULL THEN 'R1_missing_revenue'
    WHEN event_type NOT IN ({allowed_types})          THEN 'R2_invalid_event_type'
    WHEN revenue < 0                                  THEN 'R3_negative_revenue'
    WHEN event_type <> 'conversion' AND revenue <> 0  THEN 'R3_revenue_on_non_conversion'
  END AS rejection_reason
FROM {T_SOURCE}
""")

# Valid rows, one per event_id. event_date (UTC) is derived here so both models use the same date.
spark.sql(f"""
CREATE OR REPLACE VIEW {V_VALID} AS
SELECT event_id, contact_id, contact_segment, campaign_id, campaign_name, channel,
       event_type, event_timestamp, CAST(event_timestamp AS DATE) AS event_date, revenue
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY event_id
           ORDER BY event_timestamp, contact_id, campaign_id, event_type, revenue
         ) AS rn
  FROM {V_CHECKED}
  WHERE rejection_reason IS NULL
)
WHERE rn = 1
""")

print(f"Rule views ready: {V_CHECKED}, {V_VALID}")