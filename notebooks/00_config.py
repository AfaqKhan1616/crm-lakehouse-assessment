# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_config — shared parameters
# MAGIC Every other notebook runs `%run ./00_config` first.
# MAGIC Change values here only; nothing else should hard-code names or sizes.

# COMMAND ----------

# ---------- Naming ----------
CATALOG = "workspace"
SCHEMA_BRONZE = "crm_bronze"
SCHEMA_SILVER = "crm_silver"
SCHEMA_GOLD = "crm_gold"
SCHEMA_META = "crm_meta"      # run logs, benchmark results, validation results

# ---------- Generation ----------
SEED = 42
RUN_LABEL = "medium"          # "sample", "medium" or "scale"
N_EVENTS = 100_000_000        # clean, unique events before injecting bad rows
N_CONTACTS = 5_000_000
N_CAMPAIGNS = 50
START_DATE = "2026-01-01"     # inclusive
END_DATE = "2026-06-30"       # inclusive
N_DUPLICATES = 1_000          # exact copies of existing events (must be removed)
N_INVALID = 1_000             # rows that break a validity rule (must be rejected)

# All date logic uses UTC so results never depend on the session time zone.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ---------- Business definitions ----------
VALID_EVENT_TYPES = ["impression", "open", "click", "conversion"]
# Q2 = conversion count and revenue by campaign over this date range (inclusive)
Q2_START_DATE = "2026-03-01"
Q2_END_DATE = "2026-03-31"

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
T_PIPELINE_LOG = fq(SCHEMA_META, "pipeline_runs")     # every pipeline step appended: time, rows

# Views holding the validity rules (query-time logic over the wide table)
V_CHECKED = fq(SCHEMA_BRONZE, "v_events_checked")     # every source row + rejection_reason
V_VALID = fq(SCHEMA_BRONZE, "v_valid_events")         # valid + deduplicated events

# ---------- Small helpers used by several notebooks ----------
import time
from datetime import datetime, timezone

def run_sql_timed(sql):
    """Run a SQL statement and return elapsed seconds (wall clock)."""
    t0 = time.time()
    spark.sql(sql)
    return round(time.time() - t0, 2)

def table_size(table):
    """Active Delta data size and file count, read from the Delta log."""
    d = spark.sql(f"DESCRIBE DETAIL {table}").select("sizeInBytes", "numFiles").first()
    return d["sizeInBytes"], d["numFiles"]

def log_step(step, seconds, row_count=-1, notes=""):
    """Append one pipeline step to the run log (evidence for the README)."""
    row = [(RUN_LABEL, step, float(seconds), int(row_count), notes,
            datetime.now(timezone.utc).isoformat(timespec="seconds"))]
    (spark.createDataFrame(row, "run_label string, step string, seconds double, row_count long, "
                                "notes string, captured_at_utc string")
          .write.mode("append").saveAsTable(T_PIPELINE_LOG))

print(f"Config loaded: catalog={CATALOG}, run={RUN_LABEL}, events={N_EVENTS:,}, seed={SEED}")