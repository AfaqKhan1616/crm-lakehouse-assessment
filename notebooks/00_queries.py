# Databricks notebook source
# MAGIC %md
# MAGIC # 00_queries — the two reporting queries, for both models
# MAGIC
# MAGIC One place for the SQL text, so baseline, validation and benchmark run **exactly** the same queries.
# MAGIC
# MAGIC | Query | Business question |
# MAGIC |---|---|
# MAGIC | Q1 | Daily event counts by campaign and channel |
# MAGIC | Q2 | Conversion count and conversion revenue by campaign, for `Q2_START_DATE`..`Q2_END_DATE` |
# MAGIC
# MAGIC **Metric definitions** (from the brief): a conversion is an event with `event_type = 'conversion'`;
# MAGIC conversion revenue is the sum of `revenue` on those events.
# MAGIC
# MAGIC Used via `%run ./00_queries` after `%run ./00_config`.

# COMMAND ----------

def date_key(iso_date):
    """'2026-03-01' -> 20260301 (the dim_date / fact key format)."""
    return int(iso_date.replace("-", ""))

QUERIES = {
    # ---------------- Q1: daily events by campaign and channel ----------------
    "Q1_wide": f"""
        SELECT event_date, campaign_id, campaign_name, channel, COUNT(*) AS events
        FROM {V_VALID}
        GROUP BY event_date, campaign_id, campaign_name, channel""",

    "Q1_star": f"""
        SELECT d.calendar_date AS event_date, c.campaign_id, c.campaign_name, c.channel, COUNT(*) AS events
        FROM {T_FACT} f
        JOIN {T_DIM_CAMPAIGN} c ON f.campaign_id = c.campaign_id
        JOIN {T_DIM_DATE} d     ON f.date_key = d.date_key
        GROUP BY d.calendar_date, c.campaign_id, c.campaign_name, c.channel""",

    # ---------------- Q2: conversions and revenue by campaign, date range ----------------
    "Q2_wide": f"""
        SELECT campaign_id, campaign_name, COUNT(*) AS conversions, SUM(revenue) AS conversion_revenue
        FROM {V_VALID}
        WHERE event_type = 'conversion'
          AND event_date BETWEEN DATE'{Q2_START_DATE}' AND DATE'{Q2_END_DATE}'
        GROUP BY campaign_id, campaign_name""",

    "Q2_star": f"""
        SELECT c.campaign_id, c.campaign_name, COUNT(*) AS conversions, SUM(f.revenue) AS conversion_revenue
        FROM {T_FACT} f
        JOIN {T_DIM_CAMPAIGN} c ON f.campaign_id = c.campaign_id
        WHERE f.event_type = 'conversion'
          AND f.date_key BETWEEN {date_key(Q2_START_DATE)} AND {date_key(Q2_END_DATE)}
        GROUP BY c.campaign_id, c.campaign_name""",

    # ---------------- Overall business totals (for reconciliation) ----------------
    "TOTALS_wide": f"""
        SELECT COUNT(*) AS events,
               SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) AS conversions,
               SUM(CASE WHEN event_type = 'conversion' THEN revenue ELSE 0 END) AS conversion_revenue
        FROM {V_VALID}""",

    "TOTALS_star": f"""
        SELECT COUNT(*) AS events,
               SUM(CASE WHEN event_type = 'conversion' THEN 1 ELSE 0 END) AS conversions,
               SUM(CASE WHEN event_type = 'conversion' THEN revenue ELSE 0 END) AS conversion_revenue
        FROM {T_FACT}""",
}

print("Queries loaded:", ", ".join(QUERIES))
