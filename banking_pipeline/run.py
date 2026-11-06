from contextlib import closing
"""Validate a CSV batch and publish SQLite analytics and quality artifacts."""
import argparse
import csv
import hashlib
import json
import re
import sqlite3
from collections import Counter
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from .generate import FIELDS


def validate(row):
    errors = []
    if None in row or any(value is None for value in row.values()):
        return None, ['row_shape']
    clean = {key: row[key].strip() for key in FIELDS}
    for key in ('transaction_id', 'account_id', 'merchant_category'):
        if not clean[key]:
            errors.append(f'missing_{key}')
    try:
        value = clean['transaction_date']
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            raise ValueError()
        date.fromisoformat(value)
    except ValueError:
        errors.append('invalid_date')
    clean['transaction_type'] = clean['transaction_type'].lower()
    if clean['transaction_type'] not in ('credit', 'debit'):
        errors.append('invalid_type')
    clean['currency'] = clean['currency'].upper()
    if clean['currency'] != 'USD':
        errors.append('unsupported_currency')
    try:
        amount = Decimal(clean['amount'])
        if (not amount.is_finite() or amount <= 0 or amount > Decimal('1000000000')
                or amount.as_tuple().exponent < -2):
            raise ValueError()
        clean['amount_cents'] = int(amount * 100)
    except (InvalidOperation, ValueError):
        errors.append('invalid_amount')
    return (None if errors else clean), errors


def run(input_path, output_dir):
    input_path, output_dir = Path(input_path), Path(output_dir)
    # Read once so the audit fingerprint describes the bytes actually processed.
    raw = input_path.read_bytes()
    import io
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig'), newline=''))
    if reader.fieldnames != FIELDS:
        raise ValueError('CSV schema must match: ' + ', '.join(FIELDS))
    accepted, rejected, seen, reasons = [], [], set(), Counter()
    for row_number, row in enumerate(reader, start=2):
        clean, errors = validate(row)
        if clean and clean['transaction_id'] in seen:
            errors.append('duplicate_transaction_id')
        if errors:
            reasons.update(errors)
            rejected.append({**{key: row.get(key, '') for key in FIELDS},
                             'row_number': row_number,
                             'errors': ';'.join(errors),
                             'extra_values': json.dumps(row.get(None, []))})
        else:
            seen.add(clean['transaction_id'])
            accepted.append(clean)
    output_dir.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(output_dir / 'banking.sqlite')) as db, db:
        db.execute('''CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
            transaction_date TEXT NOT NULL, transaction_type TEXT NOT NULL,
            amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
            currency TEXT NOT NULL, merchant_category TEXT NOT NULL)''')
        db.execute('DELETE FROM transactions')
        db.executemany('''INSERT INTO transactions VALUES
            (:transaction_id, :account_id, :transaction_date, :transaction_type,
             :amount_cents, :currency, :merchant_category)''', accepted)
        db.execute('''CREATE VIEW IF NOT EXISTS daily_metrics AS
            SELECT transaction_date, COUNT(*) AS transaction_count,
            SUM(CASE WHEN transaction_type='credit' THEN amount_cents ELSE 0 END) AS credit_cents,
            SUM(CASE WHEN transaction_type='debit' THEN amount_cents ELSE 0 END) AS debit_cents,
            SUM(CASE WHEN transaction_type='credit' THEN amount_cents ELSE -amount_cents END) AS net_cents
            FROM transactions GROUP BY transaction_date''')
        cursor = db.execute('SELECT * FROM daily_metrics ORDER BY transaction_date')
        metrics = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
    with (output_dir / 'daily_metrics.csv').open('w', newline='', encoding='utf-8') as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(metrics)
    with (output_dir / 'rejected.csv').open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS + ['row_number', 'errors', 'extra_values'])
        writer.writeheader()
        writer.writerows(rejected)
    total = len(accepted) + len(rejected)
    report = {'input_file': input_path.name, 'input_sha256': hashlib.sha256(raw).hexdigest(),
              'total_rows': total, 'accepted_rows': len(accepted),
              'rejected_rows': len(rejected),
              'acceptance_rate': round(len(accepted) / total, 6) if total else 0,
              'rejection_reasons': dict(sorted(reasons.items()))}
    (output_dir / 'quality_report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output-dir', default='artifacts')
    args = parser.parse_args()
    try:
        report = run(args.input, args.output_dir)
    except (OSError, ValueError, csv.Error, sqlite3.Error) as exc:
        parser.exit(1, f'Pipeline failed: {exc}\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

