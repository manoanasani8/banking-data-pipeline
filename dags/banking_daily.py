"""Daily banking batch orchestration for Airflow 3 on a shared local filesystem."""

import hashlib
import json
import logging
import os
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from banking_pipeline.generate import FIELDS
from banking_pipeline.reconcile import reconcile
from banking_pipeline.run import run

LOG = logging.getLogger(__name__)


def root():
    return Path(os.environ['BANKING_PIPELINE_ROOT']).resolve()


@dag(
    dag_id='banking_daily',
    start_date=datetime(2026, 1, 1, 2, tzinfo=timezone.utc),
    schedule=CronDataIntervalTimetable('0 2 * * *', timezone='UTC'),
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'data-quality'],
)
def banking_daily():
    @task
    def select_partition():
        context = get_current_context()
        override = (context['dag_run'].conf or {}).get('partition')
        if override is not None:
            if not isinstance(override, str) or date.fromisoformat(override).isoformat() != override:
                raise AirflowFailException('partition must be YYYY-MM-DD')
            return override
        end = context.get('data_interval_end')
        if end is None:
            raise AirflowFailException('Manual runs require conf.partition (YYYY-MM-DD)')
        return (end.astimezone(timezone.utc).date() - timedelta(days=1)).isoformat()

    @task.sensor(poke_interval=60, timeout=3600, mode='reschedule')
    def wait_for_delivery(partition):
        return (root() / 'incoming' / partition / 'manifest.json').is_file()

    @task(retries=2, retry_delay=timedelta(minutes=5), pool='banking_sqlite_writer')
    def validate_load_and_reconcile(partition):
        import csv

        context = get_current_context()
        ti = context['ti']
        run_key = hashlib.sha256(ti.run_id.encode()).hexdigest()[:20]
        attempt = root() / 'attempts' / partition / run_key / str(ti.try_number)
        attempt.mkdir(parents=True, exist_ok=True)
        delivery = root() / 'incoming' / partition
        try:
            manifest = json.loads((delivery / 'manifest.json').read_text(encoding='utf-8'))
            if manifest['partition'] != partition or not isinstance(manifest['version'], str) or not manifest['version'].strip():
                raise ValueError('Manifest partition or version is invalid')
            for name in ('transactions.csv', 'account_controls.csv'):
                payload = (delivery / name).read_bytes()
                if hashlib.sha256(payload).hexdigest() != manifest['sha256'][name]:
                    raise ValueError(f'Checksum mismatch: {name}')
                (attempt / name).write_bytes(payload)
            with (attempt / 'transactions.csv').open(encoding='utf-8-sig', newline='') as handle:
                rows = csv.DictReader(handle)
                if rows.fieldnames != FIELDS or any(row['transaction_date'] != partition for row in rows):
                    raise ValueError('Transaction schema or partition does not match the delivery')
        except (ValueError, KeyError, TypeError, csv.Error) as exc:
            raise AirflowFailException(f'Invalid delivery for {partition}: {exc}') from exc

        warehouse = root() / 'warehouse'
        result = run(attempt / 'transactions.csv', warehouse, mode='incremental')
        for name in ('daily_metrics.csv', 'quality_report.json', 'rejected.csv'):
            shutil.copyfile(warehouse / name, attempt / name)
        LOG.info('Partition %s, source version %s, quality %s; artifacts: %s',
                 partition, manifest['version'], result, attempt)
        if result['rejected_rows']:
            raise AirflowFailException(f'Rejected rows require review: {attempt / "rejected.csv"}')
        try:
            exceptions = reconcile(warehouse / 'banking.sqlite', attempt / 'account_controls.csv',
                                   attempt / 'account_reconciliation.csv')
        except ValueError as exc:
            raise AirflowFailException(f'Invalid account controls: {exc}') from exc
        if exceptions:
            raise AirflowFailException(f'{exceptions} account exceptions: {attempt / "account_reconciliation.csv"}')
        return {'partition': partition, 'version': manifest['version'], 'attempt': str(attempt), 'run_key': run_key}

    @task
    def publish_report(result):
        target = root() / 'published' / result['partition'] / result['run_key']
        target.mkdir(parents=True, exist_ok=True)
        # Consumers should only read a delivery with its completion marker.
        marker = target / '_SUCCESS.json'
        if marker.exists():
            return
        for name in ('daily_metrics.csv', 'quality_report.json', 'account_reconciliation.csv'):
            shutil.copyfile(Path(result['attempt']) / name, target / name)
        temp = target / '_SUCCESS.tmp'
        temp.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
        temp.replace(marker)

    partition = select_partition()
    ready = wait_for_delivery(partition)
    checked = validate_load_and_reconcile(partition)
    ready >> checked
    publish_report(checked)


banking_daily()
