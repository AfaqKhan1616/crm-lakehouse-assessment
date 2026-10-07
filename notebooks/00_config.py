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

# ---------- Generation (filled in during Phase 1) ----------
SEED = 42
RUN_LABEL = "sample"          # "sample" or "scale"
N_EVENTS = 10_000_000         # sample size; raise for the 30 GB scale run
N_CONTACTS = 1_000_000
N_CAMPAIGNS = 200
START_DATE = "2026-01-01"
END_DATE = "2026-06-30"
N_DUPLICATES = 1_000          # known injected duplicate event_ids
N_INVALID = 1_000             # known injected invalid rows

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

print(f"Config loaded: catalog={CATALOG}, run={RUN_LABEL}, events={N_EVENTS:,}, seed={SEED}")
