# Official SDAIA Qafila Capstone — Course Overlay (Option 1)

**Author:** @Shatha-algobaishi  
**Course:** SDAIA Academy Modern Data Engineering — @SDAIAAcademy  
**Upstream:** https://github.com/SDAIAAcademy/data-engineering/tree/main/labs/capstone

This folder implements **Option 1 only** by extending the official SDAIA starter. It is not the separate offline SQLite demo located in `src/`. The baseline remains the upstream course architecture: **SQLite OLTP → DuckDB raw → dbt staging/marts → Iceberg gold**, orchestrated with **Dagster**.

## Run the official stack

1. Clone [the course repository](https://github.com/SDAIAAcademy/data-engineering).
2. Copy this folder's `capstone/pipeline.py`, `capstone/verify.py`, and `capstone/contracts/payments.odcs.yaml` over the matching files in `data-engineering/labs/capstone/`.
3. In the course `labs` directory run:

```bash
uv sync
uv run python capstone/pipeline.py
uv run python capstone/verify.py
uv run python capstone/pipeline.py
uv run python capstone/verify.py
uv run dagster dev -f capstone/pipeline.py
```

The `pipeline.py` imports `de_utils` and uses the upstream course data generator and dbt project; **copying the overlay alone will not work**. Use the upstream `labs/pyproject.toml` dependencies. The course data is generated locally and should **never** be committed.

## What changed from the starter?

- Payment and delivery-event feeds are merged into DuckDB by `payment_id`/`event_id`, keeping the newest record based on source timestamps.
- Failed payment records are isolated in `quarantine.invalid_payments` and excluded from the clean payment table used by dbt.
- Warehouse output is stable across repeated runs because records are updated by source key and timestamp, instead of replacing entire raw payment/event tables.
- ODCS payment contract, existing dbt tests, Dagster scheduling and Iceberg publication remain in the official pipeline.
- `verify.py` runs eight additional warehouse checks and records counts of source rows and quarantined records.

## Important limits

- Source JSONL files are fully re-scanned each run. Only the **warehouse writes** are incremental. This is *not* incremental API pagination or a CDC consumer.
- SQLite application tables and supplier/review sources remain full-refresh in the baseline.
- The payment quarantine and warehouse checks are an added demonstration; review access control and policy before running on any real customer data.
- `raw.customers` includes sensitive columns from the course data. Raw is local-only and is not suitable for publishing. Do not commit raw DBs, local generated data, national IDs, IBANs, phone numbers or screenshots of them.
- The baseline's payment contract check is a warning, and these changes do not formally prove that every possible dbt dependency will block on future quality failures. See the governance note.
- The complete Dagster/dbt/Iceberg integration must pass a live test against the latest upstream checkout before presenting it as a verified official end-to-end submission.

## Fact grain

| Table | One row per | Key |
|---|---|---|
| raw.payments | latest payment attempt | payment_id |
| raw.delivery_events | latest delivery event | event_id |
| marts.fct_orders | order | order_id |
| marts.fct_order_items | order item | order_id + line_no |
| marts.mart_daily_city_sales | date + city | order_date + city |

## Evaluation

| Criterion | Implementation |
|---|---|
| Functional flow | Dagster assets: 5 SQLite tables, 4 additional file feeds, dbt assets and Iceberg Gold |
| Architecture/model | Upstream dbt staging/intermediate/marts with documented grain |
| Quality | ODCS contract, dbt tests, 8 independent verification queries, payment quarantine |
| Orchestration/governance | Dagster job and nightly Asia/Riyadh schedule; governance guidance |
| Presentation | Original README, architecture, verification steps and demo instructions |

The original rubric also requires demonstrating at least one real data issue (the course payment feed contains non-positive values) and executing a repeatable run.
