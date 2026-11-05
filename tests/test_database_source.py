import shutil
import sqlite3
from src.pipeline import ROOT, run
from scripts.create_demo_db import create_demo_database

def test_real_database_plus_file_sources(tmp_path):
    inputs=tmp_path/'inputs'
    inputs.mkdir()
    create_demo_database(inputs/'orders.sqlite')
    for filename in ('payments.jsonl','events.jsonl'):
        shutil.copy(ROOT/'data'/filename,inputs/filename)
    db=tmp_path/'warehouse.sqlite'
    first=run(inputs,db,'temporary-test-key')
    assert first['ingest']['orders']['source_type']=='database'
    assert first['ingest']['payments']['source_type']=='file'
    assert not first['failures']
    with sqlite3.connect(inputs/'orders.sqlite') as source:
        source.execute("UPDATE orders SET amount=130, updated_at='2026-01-02T12:00:00Z' WHERE order_id=1")
    second=run(inputs,db,'temporary-test-key')
    assert not second['failures']
    with sqlite3.connect(db) as cn:
        assert cn.execute('SELECT amount FROM silver_orders WHERE order_id=1').fetchone()[0]==130
        assert cn.execute('SELECT sum(revenue) FROM gold_daily_city_sales').fetchone()[0]==255
    third=run(inputs,db,'temporary-test-key')
    assert third['after']==second['after']
