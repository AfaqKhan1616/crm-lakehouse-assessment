# Marketing CRM Lakehouse: Wide Table vs. Star Schema

Senior Data Engineer technical assessment. Synthetic marketing CRM events are generated in Databricks and reported two ways from the **same source data**:

1. **Baseline:** queries run directly on one wide, denormalized Delta table (no pipeline).
2. **Pipeline:** Bronze → Silver → Gold, producing a star schema (`fact_marketing_events`, `dim_contact`, `dim_campaign`, `dim_date`).

Both apply **identical validity rules** and return **identical business results**. One optimization (liquid clustering on the fact table) is then measured before and after.

**Loom walkthrough:** _TODO: add link_

---

## 1. Environment

| Item | Value |
|---|---|
| Platform | Databricks **Free Edition** |
| Compute | Serverless only (size not configurable) |
| Runtime | `19.9.x-aarch64-photon-scala2.13` (Photon) |
| Spark / Python | 4.2.0 / 3.12.3 |
| Catalog / schemas | `workspace` / `crm_bronze`, `crm_silver`, `crm_gold`, `crm_meta` |
| SQL warehouse | One 2X-Small warehouse available; **benchmarks ran from notebooks on serverless compute**, so warehouse-specific behaviour (result cache, warehouse startup) was not measured |


## 2. Repository layout and execution order

| Notebook | Purpose |
|---|---|
| `00_config` | All parameters, table names, helpers. **Change sizes only here.** |
| `00_rules` | Validity rules R1–R4 as two views (shared by baseline and pipeline) |
| `00_queries` | Q1, Q2 and totals SQL for both models (shared by baseline, validation, benchmark) |
| `00_setup` | Creates schemas, records the environment |
| `01_generate_source` | Deterministic generator → wide Delta table (source = baseline = Bronze) |
| `02_baseline_queries` | Q1, Q2, totals on the wide table |
| `03_silver` | Validate + deduplicate → Silver; rejected rows → quarantine |
| `04_gold` | Star schema; measures Gold load time |
| `05_validation` | Three checks + full-refresh rerun; saves results to `crm_meta.validation_results` |
| `06_benchmark` | Wide vs. star, before/after liquid clustering |

**Sizes:** set `RUN_LABEL` and `N_EVENTS` in `00_config`: sample `1_000_000`, medium `100_000_000` (used for all benchmarks), scale `2_600_000_000` (~30 GB). The committed config holds the last run executed (scale).

**Run:** link this repo as a Databricks Git folder, then run `00_setup` → `01` → `02` → `03` → `04` → `05` → `06` (each with *Run all*). `00_config`, `00_rules` and `00_queries` are pulled in by `%run`.

## 3. Synthetic data

**Generator design** (`01_generate_source`)
- `spark.range(N)` plus Spark expressions: rows are computed on executors, nothing is collected on the driver.
- Randomness = `xxhash64(id, SEED, salt)`, not `rand()`. The value depends only on the row id, so output is **identical regardless of partitioning** (verified: same checksum with 1 and 6 cores).
- Contact segment derives from the contact id; campaign name/channel from the campaign id → attributes are consistent per id, so current-state (SCD1) dimensions are sufficient.
- **Skew:** campaign = `floor(u³ × N_CAMPAIGNS)`; the top campaign receives ~27% of events.
- Event mix: impression 50%, open 20%, click 25%, conversion 5%. Revenue 10–500 on conversions only.
- **Known bad rows:** `N_DUPLICATES` exact copies of existing events and `N_INVALID` invalid rows (4 kinds, round-robin: null contact, null campaign, null timestamp, invalid event type).

**Parameters:** seed 42, 50 campaigns, 5 channels, 4 segments, events 2026-01-01..2026-06-30 (UTC), 1,000 duplicates, 1,000 invalid rows.

**Measured runs** (active Delta files from `DESCRIBE DETAIL`; logs and old versions excluded)

| Run | N_EVENTS | Contacts | Source rows | Active size | Files | Bytes/row | Write time |
|---|---|---|---|---|---|---|---|
| sample | 1,000,000 | 100,000 | 1,002,000 | 11.8 MB | 24 | 12.3 | not recorded |
| medium | 100,000,000 | 5,000,000 | 100,002,000 | 1,188.8 MB | 24 | 12.5 | 23.7 s |
| **scale** | 2,600,000,000 | 5,000,000 | **2,600,002,000** | **30,860.1 MB (30.1 GiB / 32.4 GB)** | 24 | 12.4 | **377.4 s** |

**30 GB target reached** (active files only). Generation scaled better than linearly: 26× the rows took 16× the time, because serverless allocated more parallelism to the larger job. The scale table has only 24 files (~1.3 GB each); in production I'd target smaller files (e.g., `repartition` or auto-compaction) to improve parallel reads and data skipping.

**Why 30 GB needs ~2.6 billion rows:** Delta/Parquet compresses this schema to ~12.5 bytes per row (few distinct segments, channels, campaigns → dictionary encoding). No padding columns were added to inflate size; the brief asks for none. Benchmarks use the **medium** run.

## 4. Validity rules (defined once, `00_rules`)

| Rule | Definition | Action |
|---|---|---|
| R1 Required | `event_id`, `contact_id`, `campaign_id`, `event_timestamp`, `event_type`, `revenue` not NULL | reject |
| R2 Allowed type | `event_type` ∈ impression, open, click, conversion | reject |
| R3 Revenue | `revenue ≥ 0`, and `revenue = 0` unless conversion | reject |
| R4 Unique | one row per `event_id`, **after** R1–R3; deterministic tie-break | drop extras |

- **Same rules in both models by construction:** the baseline queries `v_valid_events`; Silver is `v_valid_events` materialized.
- Reject first, then deduplicate, so a valid copy of an event survives even if another copy is invalid.
- **Business definitions:** a conversion is an event with `event_type = 'conversion'`; conversion revenue is the sum of `revenue` on those events. Event date = UTC date of `event_timestamp`.

## 5. Star schema (`04_gold`)

| Table | Grain | Columns |
|---|---|---|
| `fact_marketing_events` | one valid event | event_id, contact_id, campaign_id, date_key, event_type, revenue |
| `dim_contact` | one contact | contact_id, contact_segment |
| `dim_campaign` | one campaign | campaign_id, campaign_name, channel |
| `dim_date` | one calendar day | date_key, calendar_date, month, year, quarter, day_name |

Adjustments to the suggested design:
- Source ids are the dimension keys (no surrogate keys): attributes never change, so there is no history to manage.
- `dim_date` adds `quarter` and `day_name` (cheap, common filters) and covers every day between first and last event.
- The fact keeps `event_type` (needed for conversions) but not `event_timestamp`; reporting grain is daily.
- A quarantine table keeps rejected rows with their rule (optional in the brief; useful for debugging).
- Every table is built with `CREATE OR REPLACE TABLE`: a full refresh that never appends.

**Medium run sizes**

| Table | Rows | Size | Files |
|---|---|---|---|
| marketing_events_wide (source) | 100,002,000 | 1,188.8 MB | 9 |
| silver.marketing_events | 100,000,000 | 1,597.4 MB | 74 |
| fact_marketing_events | 100,000,000 | 948.6 MB | 8 |
| dim_contact | 5,000,000 | 21.2 MB | 17 |
| dim_campaign | 50 | <0.1 MB | 1 |
| dim_date | 181 | <0.1 MB | 1 |

Observations: the fact table is **20% smaller** than the wide table because campaign and contact text are not repeated per event. Silver is **larger** than the source: the dedup shuffle randomizes row order, which hurts compression, and it adds `event_date`.

## 6. Validation (`05_validation`, medium run)

| Check | What | Result |
|---|---|---|
| 0 | Brief's 5 example rows through the same rules → 3 events, 1 conversion, 120.00 | **PASS** |
| 1 Uniqueness | fact rows = distinct event_id; each dimension rows = distinct key | **PASS** |
| 2 Required + relationships | no NULL fact keys/measures; Silver re-checked against R1–R3; 0 orphan keys (LEFT ANTI JOIN) | **PASS** |
| 2b Known bad rows | rejected = 1,000 (250 per kind); duplicates removed = 1,000; valid = 100,000,000 | **PASS** |
| 3 Equivalence | Q1, Q2, totals: `EXCEPT ALL` both directions = 0 rows | **PASS** |
| Rerun | rebuild Silver + Gold: same rows, revenue and content checksum; new Delta version | **PASS** |

**41 PASS, 0 FAIL** (saved in `crm_meta.validation_results`).

Business totals, identical in both models: **100,000,000 events, 4,996,783 conversions, 1,274,871,871.44 conversion revenue**.

## 7. Benchmark (`06_benchmark`, medium run)

Wall-clock seconds of `collect()` in a serverless notebook. Each query run twice per configuration.

| Query | Config | Run 1 (s) | Run 2 (s) |
|---|---|---|---|
| Q1 daily events by campaign & channel | baseline (wide + rules at query time) | 31.71 | 29.36 |
| Q1 | star | 2.08 | 2.36 |
| Q1 | star, fact clustered by date_key | 3.27 | 2.02 |
| Q2 conversions & revenue, March | baseline | 25.52 | 24.88 |
| Q2 | star | 1.22 | 1.34 |
| Q2 | star, fact clustered by date_key | 1.71 | 1.44 |

**Star vs. baseline: 12–20× faster** for the same answers. Run 1 and run 2 differ by less than ~0.6 s, so caching does not explain the gap.

**File layout for Q2 (March, 31 of 181 days)**

| Fact table | Size | Files | Files overlapping March | Rows in those files | Avg days per file |
|---|---|---|---|---|---|
| unclustered | 948.6 MB | 8 | **8 (100%)** | 100,000,000 | 181 |
| clustered by date_key | 1,000.6 MB | 16 | **4 (25%)** | 25,961,970 | 11.3 |

Measured from `_metadata.file_path` + min/max `date_key` per file (the same statistics Delta uses for data skipping).
`system.query.history` was readable but returned no rows for notebook statements in this account, so bytes/files read per query come from this layout analysis plus the query profile in the UI.

**Processing and load time**

| Step | Seconds |
|---|---|
| Source generation (100M) | 23.7 |
| Silver build: validate + dedup 100M rows (+ quarantine 3.4 s) | 51.1 |
| Gold load: fact 10.6 + dim_contact 9.7 + dim_campaign 4.3 + dim_date 3.4 | 28.0 (first build: 32.4) |
| Clustered fact: CTAS 11.8 + OPTIMIZE FULL 30.0 | 41.8 |

Full pipeline (Silver + Gold) on 100M rows: **~79 s**. Clustering the fact adds **~42 s**, more than the whole Gold load.

## 8. Optimization: liquid clustering on `fact_marketing_events.date_key`

- **Observed problem:** events are written in random date order, so every fact file spans the full half-year. A one-month filter cannot skip any file.
- **Change:** `CLUSTER BY (date_key)` + `OPTIMIZE FULL`. Each file then covers a narrow date range, so Delta skips files using min/max statistics.
- **Result (measured):** data that Q2 must read dropped from 8 of 8 files to **4 of 16** (100M → 26M rows, about **−74%**). **Query time did not improve** (Q2: 1.22/1.34 s → 1.71/1.44 s; Q1 unchanged within noise).
- **Why no speed-up (inferred):** at ~1 GB the star query already runs in ~1–2 s, dominated by fixed costs: serverless scheduling, planning, broadcasting the dimension. Parquet already reads only the 4 needed columns. Halving a small scan saves less time than run-to-run noise.
- **Decision:** don't adopt clustering at this scale; it costs 42 s per refresh and +5% storage for no measurable gain. Revisit when the fact is large enough that scan time dominates (e.g., the 30 GB scale), where reading 25% of files should matter.
- **Cost:** 41.8 s extra at load (CTAS 11.8 s + OPTIMIZE FULL 30.0 s) versus a 10.6 s unclustered fact build, plus ongoing OPTIMIZE as data arrives.
- **Where it doesn't help:** Q1 reads every date, so it gains little. The **baseline cannot benefit**: deduplication must run over all rows before the date filter (a duplicate could fall outside the range), so the filter cannot be pushed below the window function.
- **When it's worth it:** when most queries filter on date ranges and the table is large. For a small table or full-scan reports, the maintenance cost outweighs the gain.

## 9. Cost discussion (proxies only, no dollar figures)

- **Baseline:** pays validation + dedup (a full shuffle of all rows) on **every** query. Cost grows with query frequency × table size.
- **Pipeline:** pays validation + dedup **once per refresh** (Silver build), then queries scan a narrower fact (948.6 MB vs. 1,188.8 MB) and join small dimensions (broadcast).
- **Clustering:** shifts more work to load time to reduce data read per query.
- **Break-even (measured inputs):** the pipeline costs ~79 s per refresh and saves ~24–29 s per query (baseline 25–32 s vs. star 1–2 s), so it pays for itself after **about 3 queries** per refresh. Any dashboard used more than that favours the star schema.
- **Clustering:** reduced data scanned by ~74% for date-range queries (a cost proxy on scan-billed engines) but added 42 s of load and 5% storage, with no time gain at 100M rows.

## 10. Next steps

- **Next optimization:** a pre-aggregated Gold table at `date × campaign` grain (~9,000 rows). It would answer Q1 and Q2 without scanning the 100M-row fact; the cost is one more table to refresh and keep consistent.
- **Incremental processing:** land new events in Bronze with Auto Loader; `MERGE INTO` Silver on `event_id` (dedup across batches); recompute only affected `date_key` partitions of Gold, or `MERGE` the fact. Change Data Feed on Silver can drive Gold.
- **Recovery:** Delta time travel / `RESTORE TABLE ... TO VERSION AS OF` to roll back a bad load. The validation notebook can gate publishing: run checks on new versions before reports use them.

## 11. Main limitations and uncertainty

- Serverless timings vary run to run and the disk cache could not be fully controlled; differences under ~20% should not be over-interpreted.
- Benchmarks and validation are on the 100M-row (1.2 GB) medium run. The 30 GB scale run was executed for generation and size only; running the full pipeline and benchmark at 2.6B rows was skipped to stay within the Free Edition quota and time budget. The scale run overwrote the source table, so rerunning `02`–`06` now would process 2.6B rows.
- Synthetic data has uniform dates and only 50 campaigns; real CRM data would have more skew and late-arriving events.

**Labels:** _measured_ = sections 3, 5, 6, 7 tables. _Inferred_ = explanations in 8 and 9. _Not measured_ = dollar cost, SQL warehouse behaviour.
