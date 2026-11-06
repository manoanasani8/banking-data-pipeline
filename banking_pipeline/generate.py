"""Create reproducible synthetic transactions and deliberate quality defects."""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

FIELDS = ['transaction_id', 'account_id', 'transaction_date', 'transaction_type',
          'amount', 'currency', 'merchant_category']


def generate(output, rows=1000, seed=42):
    if rows < 1:
        raise ValueError('rows must be at least 1')
    rng = random.Random(seed)
    records = []
    for i in range(rows):
        cents = rng.randint(1, 250000)
        records.append(dict(zip(FIELDS, [
            f'TXN-{i + 1:07}', f'ACCT-{rng.randint(1, 100):04}',
            (date(2026, 1, 1) + timedelta(days=rng.randrange(30))).isoformat(),
            rng.choice(['credit', 'debit']), f'{cents // 100}.{cents % 100:02}',
            'USD', rng.choice(['groceries', 'utilities', 'payroll', 'retail', 'travel'])])))
    for i, changes in enumerate([
        {}, {'amount': '-12.50'}, {'transaction_date': '2026-02-30'},
        {'account_id': ''}, {'currency': 'EUR'}, {'amount': 'NaN'}
    ]):
        row = records[0].copy()
        if i:
            row['transaction_id'] = f'BAD-{i}'
        row.update(changes)
        records.append(row)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='data/transactions.csv')
    parser.add_argument('--rows', type=int, default=1000)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    generate(args.output, args.rows, args.seed)
    print(f'Generated {args.rows} valid rows and 6 defective rows in {args.output}')


if __name__ == '__main__':
    main()
