# 10-minute demonstration and technical Q&A

## Live demo (use synthetic data)

1. **0:00–1:00 — Business problem:** unify orders, payments and courier events into trusted operational metrics.
2. **1:00–2:00 — Architecture:** sources -> keyed Bronze -> validated Silver -> Gold -> quality and quarantine.
3. **2:00–3:30 — Run:** `python -m src.pipeline --demo --db artifacts/qafila.sqlite`.
4. **3:30–5:00 — Data quality:** show one payment with non-positive amount is quarantined, while clean payments remain.
5. **5:00–6:30 — Idempotency:** re-run the same command; `before` and `after` counts remain equal.
6. **6:30–8:00 — Gold:** query daily city sales, paid payments, and event counts.
7. **8:00–9:00 — Governance:** note raw customer IDs are retained in Bronze while Silver uses tokens.
8. **9:00–10:00 — Tradeoffs:** explain SQLite/offline constraints and how the official Dagster/dbt/Iceberg starter differs.

## Commands

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python -m src.pipeline --demo --db artifacts/qafila.sqlite
python -m src.report --db artifacts/qafila.sqlite
```

## Prepared Q&A

- **Why an upsert?** Stable business keys prevent duplicates on replay; timestamps avoid an older record overwriting the latest.
- **What about very late arrivals?** 48-hour rewind allows bounded lateness, but deep backfills require explicit reprocessing or CDC.
- **What happens when data is invalid?** Non-positive payment amounts and inconsistent foreign keys are diverted to quarantine, not to analytical tables.
- **What is the grain?** Each bronze/silver table is keyed by its operational entity; Gold has one row per aggregate dimension combination.
- **Can this run as the course submission unchanged?** No. Integrate with and validate the official SDAIA course starter to demonstrate its prescribed dbt, DuckDB and Iceberg components.