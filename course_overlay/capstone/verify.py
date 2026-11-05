"""Verify Qafila capstone's final lakehouse and report reproducible metrics.

Run from labs/: uv run python capstone/verify.py
This script reads the warehouse produced by the course pipeline.
"""
from pathlib import Path
import json
import duckdb

LABS=Path(__file__).resolve().parents[1]
DB=LABS/'artifacts'/'capstone'/'qafila.duckdb'
if not DB.is_file():
    raise SystemExit(f'Missing capstone warehouse: {DB}')
con=duckdb.connect(str(DB), read_only=True)
checks={
    "orders_unique": "SELECT count(*)-count(DISTINCT order_id) FROM marts.fct_orders",
    "orders_nonempty": "SELECT CASE WHEN count(*)>0 THEN 0 ELSE 1 END FROM marts.fct_orders",
    "daily_sales_nonempty": "SELECT CASE WHEN count(*)>0 THEN 0 ELSE 1 END FROM marts.mart_daily_city_sales",
    "payments_unique": "SELECT count(*)-count(DISTINCT payment_id) FROM raw.payments",
    "payments_positive": "SELECT count(*) FROM raw.payments WHERE amount IS NULL OR amount<=0",
    "quarantined_payment_found": "SELECT CASE WHEN count(*)>0 THEN 0 ELSE 1 END FROM quarantine.invalid_payments",
    "events_unique": "SELECT count(*)-count(DISTINCT event_id) FROM raw.delivery_events",
    "payment_status": "SELECT count(*) FROM raw.payments WHERE status NOT IN ('captured','failed','refunded')",
}
output={}
try:
    for name,query in checks.items():
        output[name]=int(con.sql(query).fetchone()[0])
    output["raw_counts"]={name:con.sql(f'SELECT count(*) FROM raw.{name}').fetchone()[0]
                          for name in ("payments","delivery_events","orders")}
    output["quarantine_count"]=con.sql('SELECT count(*) FROM quarantine.invalid_payments').fetchone()[0]
    output["all_passed"]=all(v==0 for k,v in output.items() if k in checks)
finally:
    con.close()
print(json.dumps(output,indent=2))
if not output['all_passed']:
    raise SystemExit(1)
