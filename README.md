# Marketing CRM Lakehouse — Wide Table vs. Star Schema

Senior Data Engineer technical assessment. Synthetic marketing CRM events are generated in Databricks and reported two ways: directly from one wide Delta table (baseline), and from a Bronze → Silver → Gold star schema. Both must produce identical business results.

**Loom walkthrough:** _TODO: add link_

## Environment

| Item | Value |
|---|---|
| Platform | Databricks Free Edition |
| Compute | Serverless only, size not configurable |
| SQL warehouse | One, 2X-Small |
| Databricks version | _TODO: from `crm_meta.environment_info`_ |
| Spark version | _TODO_ |
| Python version | _TODO_ |
| Catalog | `workspace` (Free Edition default) |

**Limitations that affect benchmarks**
- No cluster sizing; timings depend on shared serverless capacity.
- Spark UI not available on serverless; execution metrics come from the query profile.
- Daily usage quota; exceeding it stops compute until reset.
- No billing data; cost is reported via proxies (runtime, bytes scanned, files read, shuffle).

## Repository layout

```
notebooks/
  00_config.py          shared parameters and table names
  00_setup.py           creates schemas, records environment
  01_generate_source.py synthetic wide table (baseline + Bronze)
  02_baseline_queries   reporting directly on the wide table
  03_silver.py          validation + dedup
  04_gold.py            star schema
  05_validation.py      3 checks + rerun evidence
  06_benchmark.py       timings, optimization before/after
```

## Execution order

1. Link this repo in Databricks (Workspace → Git folder).
2. Run `00_setup`.
3. Run `01` → `06` in order. Change sizes only in `00_config`.

## Dataset size

| Run | Rows | Active bytes | Files |
|---|---|---|---|
| sample | _TODO_ | _TODO_ | _TODO_ |
| scale | _TODO_ | _TODO_ | _TODO_ |

## Validity rules

_TODO (Phase 2)_

## Validation results

_TODO_

## Benchmark

_TODO_

## Next optimization / production steps / limitations

_TODO_
# crm-lakehouse-assessment
