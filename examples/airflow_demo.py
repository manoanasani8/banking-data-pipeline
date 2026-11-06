"""Run real Airflow test runs and verify publication on disk."""

import csv
import hashlib
import json
import os
import subprocess
from pathlib import Path


def delivery(root, partition, batch, controls):
    folder = root / 'incoming' / partition
    folder.mkdir(parents=True, exist_ok=True)
    manifest = {'partition': partition, 'version': 'demo-v1', 'sha256': {}}
    for name, payload in {'transactions.csv': Path(batch).read_bytes(),
                          'account_controls.csv': controls.encode()}.items():
        (folder / name).write_bytes(payload)
        manifest['sha256'][name] = hashlib.sha256(payload).hexdigest()
    (folder / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')


def test_run(partition):
    return subprocess.run(['airflow', 'dags', 'test', 'banking_daily',
                           partition + 'T02:00:00+00:00',
                           '--conf', json.dumps({'partition': partition})], check=False)


def verify(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    root = Path(os.environ['BANKING_PIPELINE_ROOT'])
    verify(not root.exists(), 'Use a fresh BANKING_PIPELINE_ROOT for this demo')
    header = 'account_id,transaction_count,credit_cents,debit_cents\n'
    delivery(root, '2026-01-01', 'examples/batch-1.csv', header + 'A1,1,10000,0\nA2,1,0,3000\n')
    delivery(root, '2026-01-02', 'examples/batch-2.csv', header + 'A1,2,10000,2500\nA2,1,0,2800\nA3,1,1500,0\n')
    good = test_run('2026-01-01')
    markers = list((root / 'published' / '2026-01-01').glob('*/_SUCCESS.json'))
    verify(good.returncode == 0 and len(markers) == 1, 'Matching controls did not publish')
    with (markers[0].parent / 'account_reconciliation.csv').open(newline='', encoding='utf-8') as handle:
        verify([r['status'] for r in csv.DictReader(handle)] == ['matched', 'matched'], 'Published report did not match')
    test_run('2026-01-02')
    verify(not list((root / 'published' / '2026-01-02').glob('*/_SUCCESS.json')), 'Mismatched controls published unexpectedly')
    reports = list((root / 'attempts' / '2026-01-02').glob('*/*/account_reconciliation.csv'))
    verify(len(reports) == 1, 'Mismatch exception report was not saved')
    with reports[0].open(newline='', encoding='utf-8') as handle:
        exceptions = [(r['account_id'], r['status'], r['delta_debit_cents'])
                      for r in csv.DictReader(handle) if r['status'] != 'matched']
    verify(exceptions == [('A2', 'total_mismatch', '200')], f'Unexpected exceptions: {exceptions}')
    summary = ('## Airflow DAG verification\n\n'
               '| Delivery | Verified outcome |\n| --- | --- |\n'
               '| Matching controls | Two accounts matched; `_SUCCESS.json` and reports published |\n'
               '| Deliberate mismatch | A2 debit delta +$2.00; exception report saved; no publication marker |\n')
    print(summary)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a', encoding='utf-8') as handle:
            handle.write(summary)


if __name__ == '__main__':
    main()
