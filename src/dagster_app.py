"""Optional Dagster asset graph for the self-contained demo.

Install pip install -e '.[orchestration]' and run:
  dagster dev -f src/dagster_app.py
This graph targets the bundled sample feeds, not the official SDAIA Dagster stack.
"""
import os
import dagster as dg
from src.pipeline import ROOT, connect, stage_ingest, stage_transform, stage_quality

DB = ROOT / 'artifacts' / 'qafila-dagster.sqlite'
DATA = ROOT / 'data'

@dg.asset(group_name='bronze')
def raw_feeds():
    with connect(DB) as con, con:
        stats=stage_ingest(con, DATA)
    return dg.MaterializeResult(metadata={key: value['bronze_total'] for key,value in stats.items()})

@dg.asset(group_name='silver_gold', deps=[raw_feeds])
def analytics_marts():
    with connect(DB) as con, con:
        stage_transform(con, os.environ.get('QAFILA_TOKEN_SALT','demo-only-change-in-production'))
        n=con.execute('SELECT count(*) FROM gold_daily_city_sales').fetchone()[0]
    return dg.MaterializeResult(metadata={'daily_city_rows': n})

@dg.asset_check(asset=analytics_marts, blocking=True)
def platform_quality():
    with connect(DB) as con, con:
        failures=stage_quality(con)
    return dg.AssetCheckResult(passed=not failures, metadata={'failures':str(failures),'checks':18})

job=dg.define_asset_job('refresh_qafila',selection=dg.AssetSelection.all())
defs=dg.Definitions(
    assets=[raw_feeds,analytics_marts],asset_checks=[platform_quality],jobs=[job],
    schedules=[dg.ScheduleDefinition(job=job,cron_schedule='30 1 * * *',execution_timezone='Asia/Riyadh')],
)
