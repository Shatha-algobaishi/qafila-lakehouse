"""Create a reproducible synthetic upstream OLTP database; no private data."""
from pathlib import Path
import csv
import sqlite3

ROOT = Path(__file__).resolve().parents[1]


def create_demo_database(target: Path, csv_source: Path = ROOT / 'data' / 'orders.csv') -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(target) as cn, csv_source.open(newline='', encoding='utf-8') as handle:
        cn.execute('''CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY, customer_id TEXT, city TEXT,
            amount REAL, updated_at TEXT)''')
        for row in csv.DictReader(handle):
            cn.execute('''INSERT INTO orders (order_id,customer_id,city,amount,updated_at)
                VALUES (?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET
                customer_id=excluded.customer_id,city=excluded.city,
                amount=excluded.amount,updated_at=excluded.updated_at''',
                (int(row['order_id']),row['customer_id'],row['city'],float(row['amount']),row['updated_at']))
    return target

if __name__ == '__main__':
    print(create_demo_database(ROOT / 'artifacts' / 'demo_orders.sqlite'))
