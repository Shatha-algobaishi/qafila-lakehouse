import json
import shutil
import sqlite3
from src.pipeline import ROOT, run

def test_end_to_end_and_idempotent(tmp_path):
    db=tmp_path/'data.sqlite'
    a=run(ROOT/'data',db,'local-test-only')
    assert not a['failures']
    assert a['checks']>=8
    assert a['quarantined']==1
    b=run(ROOT/'data',db,'local-test-only')
    assert b['after']==a['after']
    with sqlite3.connect(db) as con:
        assert con.execute('SELECT sum(revenue) FROM gold_daily_city_sales').fetchone()[0]==245
        assert con.execute('SELECT count(*) FROM silver_payments').fetchone()[0]==2
        assert con.execute('SELECT count(*) FROM quarantine').fetchone()[0]==1
        assert not any('customer_id'==x[1] for x in con.execute('PRAGMA table_info(silver_orders)'))

def test_updated_payment_is_upserted(tmp_path):
    folder=tmp_path/'input';shutil.copytree(ROOT/'data',folder)
    db=tmp_path/'data.sqlite'
    run(folder,db,'salt')
    with (folder/'payments.jsonl').open('a') as f:
        f.write(json.dumps({'payment_id':'P1','order_id':1,'amount':121,'status':'paid','updated_at':'2026-01-02T10:00:00Z'})+'\n')
    a=run(folder,db,'salt')
    assert not a['failures']
    with sqlite3.connect(db) as cn:
        assert cn.execute("SELECT amount FROM silver_payments WHERE payment_id='P1'").fetchone()[0]==121
        assert cn.execute("SELECT count(*) FROM bronze_payments WHERE payment_id='P1'").fetchone()[0]==1
