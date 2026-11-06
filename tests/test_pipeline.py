from contextlib import closing
import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path
from banking_pipeline.generate import FIELDS, generate
from banking_pipeline.run import run, validate
from banking_pipeline.reconcile import reconcile


class PipelineTests(unittest.TestCase):
    def batch(self, path, rows):
        with path.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)

    def test_incremental_replay_and_conflicts_preserve_history(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.batch(root / 'first.csv', [self.row()])
            run(root / 'first.csv', root / 'out', 'incremental')
            self.batch(root / 'second.csv', [self.row(amount='0.20'), self.row(transaction_id='T2')])
            result = run(root / 'second.csv', root / 'out', 'incremental')
            self.assertEqual((result['inserted_rows'], result['rejected_rows']), (1, 1))
            self.assertEqual(result['rejection_reasons'], {'existing_transaction_conflict': 1})
            replay = run(root / 'second.csv', root / 'out', 'incremental')
            self.assertEqual((replay['inserted_rows'], replay['replayed_rows']), (0, 1))
            with closing(sqlite3.connect(root / 'out/banking.sqlite')) as db:
                self.assertEqual(db.execute('SELECT transaction_id,amount_cents FROM transactions ORDER BY transaction_id').fetchall(), [('T1', 10), ('T2', 10)])

    def test_reconciliation_checks_both_sides_and_exact_cents(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.batch(root / 'input.csv', [self.row(), self.row(transaction_id='T2', account_id='A2'),
                                           self.row(transaction_id='T3', account_id='A3')])
            run(root / 'input.csv', root / 'out')
            controls = root / 'controls.csv'
            controls.write_text('account_id,transaction_count,credit_cents,debit_cents\nA1,1,10,0\nA2,1,11,0\nA4,1,10,0\n', encoding='utf-8')
            report = root / 'report.csv'
            self.assertEqual(reconcile(root / 'out/banking.sqlite', controls, report), 3)
            with report.open(newline='', encoding='utf-8') as handle:
                records = {row['account_id']: row for row in csv.DictReader(handle)}
            self.assertEqual({key: row['status'] for key, row in records.items()},
                             {'A1': 'matched', 'A2': 'total_mismatch', 'A3': 'unexpected_account', 'A4': 'missing_account'})
            self.assertEqual(records['A2']['delta_credit_cents'], '-1')

    def row(self, **changes):
        return dict(zip(FIELDS, ['T1', 'A1', '2026-01-01', 'credit', '0.10', 'USD', 'retail'])) | changes

    def test_money_validation(self):
        self.assertEqual(validate(self.row())[0]['amount_cents'], 10)
        for amount in ['NaN', 'Infinity', '-1', '0', '1.001', '1000000001']:
            with self.subTest(amount=amount):
                self.assertIn('invalid_amount', validate(self.row(amount=amount))[1])

    def test_strict_date(self):
        for value in ['2026-02-30', '20260101', '2026-1-1']:
            self.assertIn('invalid_date', validate(self.row(transaction_date=value))[1])

    def test_generated_quality_and_idempotence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = generate(root / 'input.csv', rows=20)
            first = run(source, root / 'out')
            self.assertEqual((first['accepted_rows'], first['rejected_rows']), (20, 6))
            self.assertEqual(first, run(source, root / 'out'))
            with closing(sqlite3.connect(root / 'out/banking.sqlite')) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM transactions').fetchone()[0], 20)

    def test_exact_aggregation_and_invalid_id_does_not_reserve(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with (root / 'input.csv').open('w', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows([self.row(amount='bad'), self.row(),
                                  self.row(transaction_id='T2', amount='0.20'),
                                  self.row(transaction_id='T3', transaction_type='debit', amount='0.05'),
                                  self.row()])
            result = run(root / 'input.csv', root / 'out')
            self.assertEqual(result['accepted_rows'], 3)
            self.assertEqual(result['rejection_reasons'], {'duplicate_transaction_id': 1, 'invalid_amount': 1})
            with closing(sqlite3.connect(root / 'out/banking.sqlite')) as db:
                self.assertEqual(db.execute('SELECT credit_cents, debit_cents, net_cents FROM daily_metrics').fetchone(), (30, 5, 25))

    def test_schema_failure_preserves_existing_batch(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = generate(root / 'input.csv', rows=3)
            run(source, root / 'out')
            source.write_text('wrong,columns\n1,2\n')
            with self.assertRaises(ValueError):
                run(source, root / 'out')
            with closing(sqlite3.connect(root / 'out/banking.sqlite')) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM transactions').fetchone()[0], 3)

    def test_empty_batch(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / 'input.csv'
            source.write_text(','.join(FIELDS) + '\n')
            self.assertEqual(run(source, root / 'out')['total_rows'], 0)


if __name__ == '__main__':
    unittest.main()

