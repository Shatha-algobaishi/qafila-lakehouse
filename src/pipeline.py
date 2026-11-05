"""Qafila Lakehouse educational pipeline: portable, incremental and testable.

No real customer data is included. SQL is stored in SQLite for an offline demo.
The official course Dagster/dbt/Iceberg starter is a separate implementation.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS ingest_state (source TEXT PRIMARY KEY, cursor TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bronze_orders (order_id INTEGER PRIMARY KEY, customer_id TEXT, city TEXT, amount REAL, updated_at TEXT);
CREATE TABLE IF NOT EXISTS bronze_payments (payment_id TEXT PRIMARY KEY, order_id INTEGER, amount REAL, status TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS bronze_events (event_id TEXT PRIMARY KEY, order_id INTEGER, event_type TEXT, event_time TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS silver_orders (order_id INTEGER PRIMARY KEY, customer_token TEXT, city TEXT, amount REAL, updated_at TEXT);
CREATE TABLE IF NOT EXISTS silver_payments (payment_id TEXT PRIMARY KEY, order_id INTEGER, amount REAL, status TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS silver_events (event_id TEXT PRIMARY KEY, order_id INTEGER, event_type TEXT, event_time TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS quarantine (source TEXT, record_id TEXT, reason TEXT, payload TEXT, PRIMARY KEY(source,record_id,reason));
CREATE TABLE IF NOT EXISTS gold_daily_city_sales (day TEXT, city TEXT, orders INTEGER, revenue REAL, PRIMARY KEY(day,city));
CREATE TABLE IF NOT EXISTS gold_order_payments (order_id INTEGER PRIMARY KEY, paid_amount REAL, payment_count INTEGER);
CREATE TABLE IF NOT EXISTS gold_delivery_performance (day TEXT, event_type TEXT, events INTEGER, PRIMARY KEY(day,event_type));
CREATE TABLE IF NOT EXISTS quality_results (check_name TEXT PRIMARY KEY, passed INTEGER, failures INTEGER, evaluated_at TEXT);
CREATE TABLE IF NOT EXISTS pipeline_runs (run_id INTEGER PRIMARY KEY AUTOINCREMENT, executed_at TEXT, outcome TEXT, metrics TEXT);
"""
SOURCES = [
    ('orders','orders.csv','order_id',('order_id','customer_id','city','amount','updated_at')),
    ('payments','payments.jsonl','payment_id',('payment_id','order_id','amount','status','updated_at')),
    ('events','events.jsonl','event_id',('event_id','order_id','event_type','event_time','updated_at')),
]

def connect(db: Path) -> sqlite3.Connection:
    db.parent.mkdir(parents=True, exist_ok=True)
    cn=sqlite3.connect(db)
    cn.row_factory=sqlite3.Row
    cn.executescript(SCHEMA)
    return cn

def read_rows(path: Path):
    if path.suffix == '.csv':
        with path.open(encoding='utf-8-sig',newline='') as file:
            yield from csv.DictReader(file)
    else:
        with path.open(encoding='utf-8') as file:
            for line in file:
                if line.strip():
                    yield json.loads(line)

def _parse_ts(value):
    if value is None:
        raise ValueError('updated_at is required')
    value=str(value)
    try:
        dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    except ValueError as exc:
        raise ValueError(f'Invalid updated_at: {value}') from exc
    if dt.tzinfo is None:
        raise ValueError('updated_at must include timezone')
    return dt.astimezone(timezone.utc)

def read_order_database(path: Path):
    uri=f'file:{path.resolve().as_posix()}?mode=ro'
    with sqlite3.connect(uri,uri=True) as source:
        source.row_factory=sqlite3.Row
        for row in source.execute('SELECT order_id,customer_id,city,amount,updated_at FROM orders'):
            yield dict(row)

def stage_ingest(cn,folder:Path,lookback_hours:int=48):
    report={}
    for source,filename,key,fields in SOURCES:
        path=folder/filename
        if source=='orders' and (folder/'orders.sqlite').is_file():
            path=folder/'orders.sqlite'
        if not path.is_file():
            raise FileNotFoundError(f'Missing source: {path}')
        last=cn.execute('SELECT cursor FROM ingest_state WHERE source=?',(source,)).fetchone()
        cursor_dt=_parse_ts(last['cursor']) if last else None
        threshold=cursor_dt-timedelta(hours=lookback_hours) if cursor_dt else None
        candidates=[]
        upstream_rows=read_order_database(path) if path.suffix=='.sqlite' else read_rows(path)
        for row in upstream_rows:
            ts=_parse_ts(row.get('updated_at'))
            if threshold is None or ts>=threshold:
                candidates.append((ts,row))
        candidates.sort(key=lambda pair:(pair[0],str(pair[1][key])))
        table='bronze_'+source
        q=','.join('?' for _ in fields)
        updates=','.join(f'{f}=excluded.{f}' for f in fields if f!=key)
        stmt=(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({q}) "
              f"ON CONFLICT({key}) DO UPDATE SET {updates} "
              f"WHERE excluded.updated_at >= {table}.updated_at")
        for ts,row in candidates:
            cn.execute(stmt,tuple(row[f] for f in fields))
        if candidates:
            new_cursor=max([d for d,_ in candidates]+([cursor_dt] if cursor_dt else []))
            cn.execute('INSERT INTO ingest_state VALUES(?,?) ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor',
                       (source,new_cursor.isoformat()))
        report[source]={'source_type':'database' if path.suffix=='.sqlite' else 'file',
                        'candidates':len(candidates),'bronze_total':cn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]}
    return report

def _safe_quarantine(cn,source,key,reason,row):
    allowed={'order_id','payment_id','event_id','amount','city','status','event_type','updated_at'}
    payload={k:row[k] for k in allowed if k in row.keys()}
    cn.execute('INSERT OR IGNORE INTO quarantine VALUES(?,?,?,?)',
               (source,str(key),reason,json.dumps(payload,sort_keys=True)))

def stage_transform(cn,token_salt:str):
    if not token_salt:
        raise ValueError('Token salt is required')
    for name in ('silver_orders','silver_payments','silver_events','quarantine',
                 'gold_daily_city_sales','gold_order_payments','gold_delivery_performance'):
        cn.execute(f'DELETE FROM {name}')
    for row in cn.execute('SELECT * FROM bronze_orders').fetchall():
        if not row['city'] or not str(row['city']).strip() or row['amount'] is None or row['amount']<0:
            _safe_quarantine(cn,'orders',row['order_id'],'missing city or invalid amount',row)
            continue
        token=hashlib.sha256((token_salt+':'+str(row['customer_id'])).encode()).hexdigest()[:24]
        cn.execute('INSERT INTO silver_orders VALUES(?,?,?,?,?)',
                   (row['order_id'],token,row['city'],row['amount'],row['updated_at']))
    valid_orders={r[0] for r in cn.execute('SELECT order_id FROM silver_orders')}
    for row in cn.execute('SELECT * FROM bronze_payments').fetchall():
        if row['order_id'] not in valid_orders or row['amount'] is None or row['amount']<=0 or row['status'] not in ('paid','pending','failed'):
            _safe_quarantine(cn,'payments',row['payment_id'],'orphan, invalid amount or status',row)
            continue
        cn.execute('INSERT INTO silver_payments VALUES(?,?,?,?,?)',tuple(row))
    for row in cn.execute('SELECT * FROM bronze_events').fetchall():
        if row['order_id'] not in valid_orders or row['event_type'] not in ('created','picked_up','delivered'):
            _safe_quarantine(cn,'events',row['event_id'],'orphan or invalid event type',row)
            continue
        cn.execute('INSERT INTO silver_events VALUES(?,?,?,?,?)',tuple(row))
    cn.execute("""INSERT INTO gold_daily_city_sales
        SELECT substr(updated_at,1,10),city,count(*),round(sum(amount),2)
        FROM silver_orders GROUP BY 1,2""")
    cn.execute("""INSERT INTO gold_order_payments
        SELECT order_id,round(sum(amount),2),count(*) FROM silver_payments
        WHERE status='paid' GROUP BY order_id""")
    cn.execute("""INSERT INTO gold_delivery_performance
        SELECT substr(event_time,1,10),event_type,count(*)
        FROM silver_events GROUP BY 1,2""")

CHECKS = {
 'bronze_orders_unique': 'SELECT count(*)-count(DISTINCT order_id) FROM bronze_orders',
 'bronze_payments_unique': 'SELECT count(*)-count(DISTINCT payment_id) FROM bronze_payments',
 'bronze_events_unique': 'SELECT count(*)-count(DISTINCT event_id) FROM bronze_events',
 'silver_orders_not_empty': 'SELECT CASE WHEN count(*)>0 THEN 0 ELSE 1 END FROM silver_orders',
 'silver_orders_nonnegative': 'SELECT count(*) FROM silver_orders WHERE amount<0 OR amount IS NULL',
 'silver_city_required': "SELECT count(*) FROM silver_orders WHERE city IS NULL OR trim(city)=''",
 'silver_tokens_required': "SELECT count(*) FROM silver_orders WHERE customer_token IS NULL OR length(customer_token)<>24",
 'silver_payments_positive': 'SELECT count(*) FROM silver_payments WHERE amount<=0 OR amount IS NULL',
 'silver_payments_status': "SELECT count(*) FROM silver_payments WHERE status NOT IN ('paid','pending','failed') OR status IS NULL",
 'silver_payments_fk': 'SELECT count(*) FROM silver_payments p LEFT JOIN silver_orders o USING(order_id) WHERE o.order_id IS NULL',
 'silver_events_fk': 'SELECT count(*) FROM silver_events e LEFT JOIN silver_orders o USING(order_id) WHERE o.order_id IS NULL',
 'silver_events_valid': "SELECT count(*) FROM silver_events WHERE event_type NOT IN ('created','picked_up','delivered')",
 'gold_sales_positive': 'SELECT count(*) FROM gold_daily_city_sales WHERE revenue<0 OR orders<=0',
 'gold_sales_reconciled': 'SELECT CASE WHEN abs((SELECT coalesce(sum(revenue),0) FROM gold_daily_city_sales)-(SELECT coalesce(sum(amount),0) FROM silver_orders))<0.001 THEN 0 ELSE 1 END',
 'gold_payments_reconciled': "SELECT CASE WHEN abs((SELECT coalesce(sum(paid_amount),0) FROM gold_order_payments)-(SELECT coalesce(sum(amount),0) FROM silver_payments WHERE status='paid'))<0.001 THEN 0 ELSE 1 END",
 'gold_delivery_reconciled': 'SELECT CASE WHEN (SELECT coalesce(sum(events),0) FROM gold_delivery_performance)=(SELECT count(*) FROM silver_events) THEN 0 ELSE 1 END',
 'privacy_no_id_in_silver': "SELECT count(*) FROM pragma_table_info('silver_orders') WHERE name IN ('customer_id','national_id','iban','mobile')",
 'privacy_no_id_in_quarantine': "SELECT count(*) FROM quarantine WHERE payload LIKE '%customer_id%' OR payload LIKE '%national_id%' OR payload LIKE '%iban%' OR payload LIKE '%mobile%'",
}

def stage_quality(cn):
    now=datetime.now(timezone.utc).isoformat()
    failures={}
    for name,query in CHECKS.items():
        count=int(cn.execute(query).fetchone()[0])
        cn.execute('INSERT INTO quality_results VALUES(?,?,?,?) ON CONFLICT(check_name) DO UPDATE SET passed=excluded.passed, failures=excluded.failures,evaluated_at=excluded.evaluated_at',
                   (name,int(count==0),count,now))
        if count: failures[name]=count
    return failures

def run(input_dir:Path,db:Path,salt:str,lookback_hours:int=48):
    with connect(db) as cn:
        before={s:cn.execute(f'SELECT count(*) FROM bronze_{s}').fetchone()[0] for s,*_ in SOURCES}
        with cn:
            ingest=stage_ingest(cn,input_dir,lookback_hours)
            stage_transform(cn,salt)
            failures=stage_quality(cn)
            after={s:cn.execute(f'SELECT count(*) FROM bronze_{s}').fetchone()[0] for s,*_ in SOURCES}
            report={'ingest':ingest,'before':before,'after':after,
                    'quarantined':cn.execute('SELECT count(*) FROM quarantine').fetchone()[0],
                    'checks':len(CHECKS),'failures':failures,
                    'gold_sales_rows':cn.execute('SELECT count(*) FROM gold_daily_city_sales').fetchone()[0],
                    'gold_payments_rows':cn.execute('SELECT count(*) FROM gold_order_payments').fetchone()[0],
                    'gold_event_rows':cn.execute('SELECT count(*) FROM gold_delivery_performance').fetchone()[0]}
            cn.execute('INSERT INTO pipeline_runs(executed_at,outcome,metrics) VALUES(?,?,?)',
                       (datetime.now(timezone.utc).isoformat(),'FAILED' if failures else 'PASS',json.dumps(report)))
    return report

def main():
    p=argparse.ArgumentParser(description='Qafila offline lakehouse demo')
    p.add_argument('--demo',action='store_true',help='Use synthetic sample data')
    p.add_argument('--input-dir',type=Path)
    p.add_argument('--db',type=Path,default=ROOT/'artifacts'/'qafila.sqlite')
    p.add_argument('--salt',default=os.environ.get('QAFILA_TOKEN_SALT','demo-only-change-in-production'))
    p.add_argument('--lookback-hours',type=int,default=48)
    args=p.parse_args()
    if not args.demo and not args.input_dir:p.error('Use --demo or --input-dir')
    if args.lookback_hours<0:p.error('--lookback-hours must be nonnegative')
    if not args.demo and args.salt=='demo-only-change-in-production':
        p.error('For non-demo data, set a private QAFILA_TOKEN_SALT')
    if args.demo:
        from scripts.create_demo_db import create_demo_database
        import shutil
        create_demo_database(ROOT/'artifacts'/'demo_orders.sqlite')
        demo_dir=ROOT/'artifacts'/'demo_inputs'
        demo_dir.mkdir(parents=True,exist_ok=True)
        shutil.copy2(ROOT/'artifacts'/'demo_orders.sqlite',demo_dir/'orders.sqlite')
        for feed in ('payments.jsonl','events.jsonl'):
            shutil.copy2(ROOT/'data'/feed,demo_dir/feed)
        input_dir=demo_dir
    else:
        input_dir=args.input_dir
    report=run(input_dir,args.db,args.salt,args.lookback_hours)
    print(json.dumps(report,indent=2))
    if report['failures']:raise SystemExit(1)

if __name__=='__main__':
    main()
