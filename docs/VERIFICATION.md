# Verification and scope

## Verified locally

- Command: `python -m pytest -q`
- Result: **7 passed**
- Executable checks: **18 SQL quality checks**
- Invalid synthetic record: payment `P3` (negative amount) is quarantined.
- Re-run stability: tests check counts and duplicate prevention.
- Database source: one test uses a separate SQLite OLTP database; payment and courier-event inputs are JSONL.

## GitHub CI

This repository includes `.github/workflows/tests.yml`, running the tests and a demo pipeline on push. Check the **Actions** tab for the latest live outcome rather than treating this document as proof of GitHub CI success.

## Important limits

The sample is a **SQLite educational implementation using synthetic data**. It is **not verified on the official SDAIA DuckDB/dbt/Dagster/Iceberg starter**. The optional Dagster module was not validated with a running Dagster instance. The actual course dataset and orchestration integrations require separate testing. Do not represent this implementation as a fully verified official capstone submission.

Official source: https://github.com/SDAIAAcademy/data-engineering/tree/main/labs/capstone
