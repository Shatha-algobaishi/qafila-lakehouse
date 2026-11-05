"""Print summary of an already-created local demo database."""
import argparse
import json
import sqlite3
from pathlib import Path
from src.pipeline import ROOT

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',type=Path,default=ROOT/'artifacts'/'qafila.sqlite')
    args=p.parse_args()
    if not args.db.exists():p.error('Run the pipeline first')
    con=sqlite3.connect(args.db)
    con.row_factory=sqlite3.Row
    def table(name):return [dict(row) for row in con.execute(f'SELECT * FROM {name}')]
    print(json.dumps({'sales':table('gold_daily_city_sales'),'payments':table('gold_order_payments'),
                      'delivery':table('gold_delivery_performance'),
                      'failed_checks':[row for row in table('quality_results') if not row['passed']],
                      'quarantined':con.execute('SELECT count(*) FROM quarantine').fetchone()[0]},indent=2))
if __name__=='__main__':main()
