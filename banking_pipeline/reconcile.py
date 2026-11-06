"""Compare warehouse account totals with independent control totals."""
import argparse
import csv
import re
import sqlite3
from contextlib import closing
from pathlib import Path


FIELDS = ['account_id', 'transaction_count', 'credit_cents', 'debit_cents']


def reconcile(database, controls, output):
    expected = {}
    with Path(controls).open(encoding='utf-8-sig', newline='') as handle:
        rows = csv.DictReader(handle)
        if rows.fieldnames != FIELDS:
            raise ValueError('Control CSV must contain: ' + ','.join(FIELDS))
        for line, row in enumerate(rows, 2):
            account = (row['account_id'] or '').strip()
            if (not account or account in expected or None in row
                    or any(not re.fullmatch(r'[0-9]{1,18}', row[key] or '') for key in FIELDS[1:])):
                raise ValueError(f'Invalid or duplicate control row {line}')
            expected[account] = tuple(int(row[key]) for key in FIELDS[1:])
    uri = Path(database).resolve().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True)) as db:
        actual = {key: tuple(totals) for key, *totals in db.execute('''
            SELECT account_id, COUNT(*),
                   SUM(CASE WHEN transaction_type='credit' THEN amount_cents ELSE 0 END),
                   SUM(CASE WHEN transaction_type='debit' THEN amount_cents ELSE 0 END)
            FROM transactions GROUP BY account_id''')}
    report = []
    for key in sorted(expected.keys() | actual.keys()):
        want, got = expected.get(key), actual.get(key)
        status = ('unexpected_account' if want is None else 'missing_account' if got is None
                  else 'matched' if want == got else 'total_mismatch')
        deltas = tuple(b - a for a, b in zip(want, got)) if want is not None and got is not None else (None,) * 3
        report.append((key, status, *(want or (None,) * 3), *(got or (None,) * 3), *deltas))
    output = Path(output)
    if output.resolve() in {Path(database).resolve(), Path(controls).resolve()}:
        raise ValueError('Output must differ from database and controls')
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['account_id', 'status'] + [f'{prefix}_{key}' for prefix in
                         ('expected', 'actual', 'delta') for key in FIELDS[1:]])
        writer.writerows(report)
    return sum(row[1] != 'matched' for row in report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--controls', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        exceptions = reconcile(args.database, args.controls, args.output)
    except (OSError, ValueError, csv.Error, sqlite3.Error) as exc:
        parser.exit(1, f'Reconciliation failed: {exc}\n')
    print(f'Account exceptions: {exceptions}. Report: {args.output}')
    if exceptions:
        parser.exit(2)


if __name__ == '__main__':
    main()
