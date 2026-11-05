# Qafila Lakehouse | SDAIA Data Engineering Capstone

**Graduation project — Option 1: Qafila Lakehouse**  
**Author:** @Shatha-algobaishi  
**Training academy:** @SDAIAAcademy  
**Course:** Modern Data Engineering for AI Systems

This repository contains a course-based implementation of the **Qafila Lakehouse** option from the [SDAIA Academy official capstone starter](https://github.com/SDAIAAcademy/data-engineering/tree/main/labs/capstone).

## Start here: official SDAIA implementation

**[`course_overlay/` — official capstone code and instructions](course_overlay/README.md)**

It extends the original pipeline rather than building another unrelated project. The integration uses:

| Stage | Technology | Function |
|---|---|---|
| Sources | SQLite OLTP, JSONL and CSV | Orders, payments, deliveries, suppliers, reviews |
| Ingestion and raw | DuckDB | Multiple sources and incremental payment/event upserts |
| Transformation | dbt | Staging, intermediate, dimensional and marts models |
| Validation | dbt tests, ODCS and SQL checks | Quality checks and non-positive payment quarantine |
| Publication | Apache Iceberg | Governed Gold data |
| Orchestration | Dagster | Asset graph, job, monitoring and scheduled refresh |

### What's improved over the starter

- Payments and delivery events update by stable source key and timestamp; reruns do not duplicate these rows.
- Invalid payments are quarantined and excluded from clean payment data before analytics.
- Eight additional warehouse verification queries in `course_overlay/capstone/verify.py`.
- An integration workflow clones the official SDAIA course repository, overlays the modifications, runs the original Dagster/dbt/Iceberg stack, and repeats the run for replay verification.

**[Official-stack validation on GitHub Actions](../../actions/workflows/course-integration.yml)**

### Reproduce against the official course

Clone the official [SDAIA course repository](https://github.com/SDAIAAcademy/data-engineering), then copy these overlay files into its `labs/capstone` directory. From the **course's** `labs` directory:

```bash
uv sync
uv run python capstone/pipeline.py
uv run python capstone/verify.py
uv run python capstone/pipeline.py
uv run python capstone/verify.py
uv run dagster dev -f capstone/pipeline.py
```

For the precise instructions and limitations, read [course_overlay/README.md](course_overlay/README.md).

## Additional offline demo (not the official course stack)

The `src/`, `data/` and `tests/` folders are a separate, portable **SQLite teaching demo** using entirely synthetic sample data. It demonstrates incremental loads, Bronze/Silver/Gold processing, row-level quarantine and 18 SQL rules. It is provided as supporting work, **not** as a replacement for the official DuckDB/dbt/Dagster/Iceberg implementation.

```bash
python -m src.pipeline --demo
python -m src.report
python -m pip install -e '.[test]'
python -m pytest -q
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), [docs/GOVERNANCE.md](docs/GOVERNANCE.md), [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md), [docs/VERIFICATION.md](docs/VERIFICATION.md) and [docs/PRESENTATION.md](docs/PRESENTATION.md). Those supporting documents originally describe the offline demo; the course-based overlay is documented separately.

## Quality, privacy and limitations

The original course dataset contains deliberately dirty records. Failed quality checks may be warnings rather than blockers; retain the actual dbt warnings in the presentation, do not claim they all passed. The incremental implementation re-scans the full JSONL file (incremental **warehouse updates**, not an API/CDC consumer). SQLite app tables and supplier/review inputs are full refresh. Generated databases and customer PII must never be pushed to GitHub. Access to raw data must be restricted.

**Testing disclosure:** The current status of the complete official stack must be taken from [the course integration workflow](../../actions/workflows/course-integration.yml). A green offline demo workflow alone does not establish that the official course flow passed.

**Source and attribution:** Based on SDAIA Academy's course starter, with modifications documented in this repository.
