import json
import shutil
import sqlite3
import pytest
from src.pipeline import ROOT,run

def test_eighteen_quality_checks_and_protected_quarantine(tmp_path):
    db=tmp_path/'d.sqlite'
    result=run(ROOT/'data',db,'unit-test-salt')
    assert result['checks']==18
    assert result['quarantined']==1
    assert result['gold_event_rows']>=1
    assert result['failures']=={}
    with sqlite3.connect(db) as con:
        assert con.execute('SELECT count(*) FROM quality_results WHERE passed=1').fetchone()[0]==18
        assert con.execute('SELECT count(*) FROM pipeline_runs').fetchone()[0]==1
        assert 'customer_id' not in con.execute('SELECT payload FROM quarantine').fetchone()[0]

def test_late_arrival_and_stale_update_do_not_overwrite(tmp_path):
    input_dir=tmp_path/'in'
    shutil.copytree(ROOT/'data',input_dir)
    db=tmp_path/'lake.sqlite'
    run(input_dir,db,'unit-test-salt')
    with (input_dir/'payments.jsonl').open('a') as f:
        f.write(json.dumps({'payment_id':'P4','order_id':3,'amount':45,'status':'paid','updated_at':'2026-01-01T09:00:00Z'})+'\n')
        f.write(json.dumps({'payment_id':'P1','order_id':1,'amount':999,'status':'paid','updated_at':'2026-01-01T07:00:00Z'})+'\n')
    r=run(input_dir,db,'unit-test-salt')
    assert r['failures']=={}
    with sqlite3.connect(db) as con:
        assert con.execute("SELECT amount FROM silver_payments WHERE payment_id='P1'").fetchone()[0]==120
        assert con.execute("SELECT amount FROM silver_payments WHERE payment_id='P4'").fetchone()[0]==45
    again=run(input_dir,db,'unit-test-salt')
    assert again['after']==r['after']

def test_transaction_rolls_back_on_invalid_timestamp(tmp_path):
    input_dir=tmp_path/'in'
    shutil.copytree(ROOT/'data',input_dir)
    db=tmp_path/'test.sqlite'
    run(input_dir,db,'unit-test-salt')
    with (input_dir/'payments.jsonl').open('a') as file:
        file.write(json.dumps({'payment_id':'P4','order_id':1,'amount':12,'status':'paid','updated_at':'wrong'})+'\n')
    with pytest.raises(ValueError,match='Invalid updated_at'):
        run(input_dir,db,'unit-test-salt')
    with sqlite3.connect(db) as con:
        assert con.execute("SELECT count(*) FROM bronze_payments WHERE payment_id='P4'").fetchone()[0]==0
        assert con.execute('SELECT count(*) FROM pipeline_runs').fetchone()[0]==1

def test_missing_file_fails(tmp_path):
    with pytest.raises(FileNotFoundError):
        run(tmp_path,tmp_path/'test.sqlite','unit-test-salt')
