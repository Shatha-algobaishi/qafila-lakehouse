# Qafila Lakehouse — Capstone presentation outline

**Academy:** @SDAIAAcademy  
**Project:** Option 1 — Qafila Lakehouse  
**Author:** @Shatha-algobaishi

1. **Title and project goal** — a reusable, quality-controlled analytics platform.
2. **Problem and sources** — SQLite OLTP orders, payments JSONL and courier-event JSONL.
3. **Architecture** — sources → Bronze → Silver / quarantine → Gold → checks and reports.
4. **Incremental ingestion** — primary-key upserts, timestamp cursor, 48-hour lookback, replay safety.
5. **Data model** — order/payment/event grain and the three Gold aggregate tables.
6. **Data quality** — 18 checks, negative-payment quarantine, referential integrity, reconciliation.
7. **Privacy and governance** — protected customer tokens, access policy, erasure considerations.
8. **Live demonstration and limitations** — run demo, rerun, inspect Gold output and tests; disclose that official Dagster/dbt/DuckDB/Iceberg stack remains unverified.

## Live demonstration

```bash
python -m src.pipeline --demo
python -m src.report
python -m pip install -e '.[test]'
python -m pytest -q
```

The PowerPoint presentation is not included in this repository; this file provides the presentation outline.
