# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config — shared parameters
# MAGIC Every other notebook runs `%run ./00_config` first.
# MAGIC Change values here only; nothing else should hard-code names or sizes.

# COMMAND ----------

# ---------- Naming ----------
# Free Edition gives you a default catalog called `workspace`.
# Creating new catalogs may be restricted there, so schemas go inside it.
CATALOG = "workspace"
SCHEMA_BRONZE = "crm_bronze"
SCHEMA_SILVER = "crm_silver"
SCHEMA_GOLD = "crm_gold"
SCHEMA_META = "crm_meta"      # run logs, benchmark results, validation results

# ---------- Generation ----------
# Develop on the sample first. For the 30 GB scale run change only RUN_LABEL and
# N_EVENTS (01_generate_source prints the N_EVENTS needed for 30 GB).
SEED = 42
RUN_LABEL = "sample"          # "sample" or "scale"
N_EVENTS = 1_000_000          # clean, unique events before injecting bad rows
N_CONTACTS = 100_000
N_CAMPAIGNS = 50
START_DATE = "2026-01-01"     # inclusive
END_DATE = "2026-06-30"       # inclusive
N_DUPLICATES = 1_000          # exact copies of existing events (must be removed)
N_INVALID = 1_000             # rows that break a validity rule (must be rejected)

# All date logic (event date -> date_key) uses UTC so results never depend on
# the session's local time zone.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ---------- Fully qualified table names ----------
def fq(schema, table):
    return f"{CATALOG}.{schema}.{table}"

T_SOURCE = fq(SCHEMA_BRONZE, "marketing_events_wide")   # baseline + Bronze
T_SILVER = fq(SCHEMA_SILVER, "marketing_events")
T_QUARANTINE = fq(SCHEMA_SILVER, "marketing_events_rejected")
T_FACT = fq(SCHEMA_GOLD, "fact_marketing_events")
T_DIM_CONTACT = fq(SCHEMA_GOLD, "dim_contact")
T_DIM_CAMPAIGN = fq(SCHEMA_GOLD, "dim_campaign")
T_DIM_DATE = fq(SCHEMA_GOLD, "dim_date")
T_ENV = fq(SCHEMA_META, "environment_info")
T_GEN_PARAMS = fq(SCHEMA_META, "generation_params")   # latest run: expected validation results
T_GEN_LOG = fq(SCHEMA_META, "generation_runs")        # every run appended: size, time

print(f"Config loaded: catalog={CATALOG}, run={RUN_LABEL}, events={N_EVENTS:,}, seed={SEED}")