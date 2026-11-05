# Official SDAIA integration results — 5 November 2026

**Run:** https://github.com/Shatha-algobaishi/qafila-lakehouse/actions/runs/42036566446  
**Status:** SUCCESS (Ubuntu runner, checked out latest course sources during workflow).

The CI workflow checked out the original `SDAIAAcademy/data-engineering` source, copied our overlay to `labs/capstone`, installed the official `labs/pyproject.toml` environment with `uv sync`, then performed two consecutive complete executions.

| Evidence | First run | Re-run |
|---|---|---|
| Dagster | Succeeded | Succeeded |
| Assets materialized | 30 | 30 |
| Dagster checks evaluated | 45 | 45 |
| dbt test/model results | PASS=63, WARN=3, ERROR=0 | PASS=63, WARN=3, ERROR=0 |
| Extra independent SQL checks | 8 of 8 passed | 8 of 8 passed |
| Quarantined non-positive/invalid payments | 16 | 16 |
| CI job | Successful | Successful |

### Known data warnings (not silently corrected)

- `positive_stg_order_items_quantity`: 142 rows.
- `positive_stg_supplier_prices_unit_cost_sar`: 4 rows.
- `assert_order_totals_match_payments`: 244 rows.

These warnings originate in the course's intentionally imperfect data and/or the current model reconciliation logic. They remain visible for review; the workflow was successful with 3 warnings, **not** 66 clean passes.

### Honest project scope

The official starter is materially improved via incremental *warehouse writes* on payment and courier event feeds plus quarantine. The source snapshots are still fully scanned, and most other raw feeds remain full-refresh. No production credentials, personally identifying customer records or data files are committed. Further hardening would be needed for a production system (API cursor, true CDC, robust secrets and row-level access, deletion, alert routing).

The course overlay runs successfully on generated course data in CI, not on real production customer data.
