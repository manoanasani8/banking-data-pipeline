# Banking Transaction Data Pipeline

[![Pipeline tests](https://github.com/manoanasani8/banking-data-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/manoanasani8/banking-data-pipeline/actions/workflows/tests.yml)

A local data engineering project that turns synthetic bank transactions into validated records, a quality report, and daily analytics in SQLite.

Built as a portfolio project by **Mano Anasani**. Uses Python's standard library, so no cloud account, API key, or third-party package is required.

## Sample results

The seeded demo loaded **1,000 valid transactions** and rejected **6 deliberately defective records**. Here are the first three days of the SQLite summary, converted from integer cents to USD for display:

| Date | Transactions | Credits | Debits | Net flow |
| --- | ---: | ---: | ---: | ---: |
| 2026-01-01 | 26 | $14,510.01 | $14,147.58 | $362.43 |
| 2026-01-02 | 39 | $18,572.82 | $27,055.16 | -$8,482.34 |
| 2026-01-03 | 34 | $23,735.91 | $18,649.78 | $5,086.13 |

For example, transaction `BAD-1` was written to `rejected.csv` with reason `invalid_amount` because its amount was `-12.50`. It was excluded from the daily totals.

## What it demonstrates

- CSV ingestion and explicit data contracts
- Deterministic synthetic data with intentional quality defects
- Validation, duplicate detection, and rejected-record auditing
- Atomic, idempotent SQLite batch loading
- SQL aggregations and operational reporting
- Automated tests for money handling, rejected data, and repeat runs

## Data flow

```mermaid
flowchart LR
    A[Synthetic transaction CSV] --> B[Validate and normalize]
    B --> C[Accepted transactions]
    B --> D[Rejected rows with reasons]
    C --> E[SQLite transaction table]
    E --> F[Daily SQL metrics]
    B --> G[JSON quality report]
```

## Run it

Install Python 3.10 or later. From this repository folder:

```sh
python -m banking_pipeline.generate --output data/transactions.csv --rows 1000 --seed 42
python -m banking_pipeline.run --input data/transactions.csv --output-dir artifacts
python -m unittest discover -s tests -v
```

On Windows, `py` can be used instead of `python` if needed.

The generator adds six intentionally defective rows after the requested number of valid rows. All names and account identifiers are fictional. No real customer data is included.

## Outputs

| File | Purpose |
| --- | --- |
| `artifacts/banking.sqlite` | Queryable accepted transaction dataset and daily metrics view |
| `artifacts/daily_metrics.csv` | Daily transaction counts, credit/debit totals and net flow |
| `artifacts/rejected.csv` | Original rejected rows plus validation reasons |
| `artifacts/quality_report.json` | Input fingerprint, row counts, acceptance rate and rejection reasons |

The default `snapshot` mode replaces the local table with the accepted rows in the input. Database writes are transactional. Exports are regenerated afterward and represent the latest run; they are not committed with the database.

## Incremental loading

```sh
python -m banking_pipeline.run --input data/transactions.csv --output-dir artifacts --mode incremental
```

Use the same output directory for successive batches. New transaction IDs are inserted; identical existing rows are counted as replays; changed records for an existing ID are rejected with `existing_transaction_conflict`. Existing rows are never silently overwritten. The report separates inserted, replayed, and rejected rows. Daily metrics reflect all stored transactions. Do not switch back to snapshot mode on this database unless you intend to replace its contents.

## Account reconciliation

Prepare an independent source control CSV with `account_id,transaction_count,credit_cents,debit_cents`. Supply one row per account using totals for the same complete period and transaction population as the SQLite database. Do not generate the controls from the SQLite table being checked.

```sh
python -m banking_pipeline.reconcile --database artifacts/banking.sqlite --controls data/account_controls.csv --output artifacts/account_reconciliation.csv
```

The report identifies matched accounts, mismatched totals, missing accounts, and unexpected accounts. Deltas are actual minus expected. The command returns 0 for a match, 2 for reconciliation exceptions, and 1 for an input or processing error.

### Sample reconciliation results

This example loads two batches into a fresh database (four synthetic transactions total) and compares the results against a separate controls fixture. Use an unused `artifacts/reconciliation-demo` directory for the first run.

```sh
python -m banking_pipeline.run --mode incremental --input examples/batch-1.csv --output-dir artifacts/reconciliation-demo
python -m banking_pipeline.run --mode incremental --input examples/batch-2.csv --output-dir artifacts/reconciliation-demo
python -m banking_pipeline.reconcile --database artifacts/reconciliation-demo/banking.sqlite --controls examples/account-controls.csv --output artifacts/reconciliation-demo/account_reconciliation.csv
```

| Account | Result | Evidence |
| --- | --- | --- |
| A1 | Matched | Two transactions; $100.00 credit and $25.00 debit agree |
| A2 | Total mismatch | $30.00 actual debit vs. $28.00 expected; +$2.00 delta |
| A3 | Unexpected account | $15.00 actual credit with no control row |
| A4 | Missing account | $12.00 expected credit with no loaded transaction |

The demo intentionally returns **exit code 2** and writes three exceptions. Replaying the same two batches does not add rows.

## Airflow scheduling

The [Airflow 3 DAG](dags/banking_daily.py) waits for a delivery manifest, checks input hashes, loads transactions incrementally, reconciles accounts, and publishes reports only after the checks pass. See the [deployment and delivery instructions](docs/airflow-design.md). The core pipeline and demo still run without Airflow.

## Data contract

Required columns: `transaction_id`, `account_id`, `transaction_date`, `transaction_type`, `amount`, `currency`, `merchant_category`.

- IDs and merchant categories must be nonempty.
- Dates use strict `YYYY-MM-DD` formatting.
- Types are `credit` or `debit`; case is normalized.
- Amounts must be positive, finite USD values with at most two decimal places, up to $1 billion per transaction. They are stored as **integer cents**, avoiding floating-point currency arithmetic.
- Only USD is supported; currency case is normalized.
- The first **valid** occurrence of a transaction ID wins. Later valid occurrences are rejected as duplicates. An invalid row does not reserve its ID.
- Unexpected columns or a different column order are rejected at the file level to make schema drift visible. Missing or extra values in an individual row are rejected at the row level.

## Example SQL

```sql
SELECT * FROM daily_metrics ORDER BY transaction_date;

SELECT merchant_category,
       COUNT(*) AS transactions,
       SUM(amount_cents) AS total_cents
FROM transactions
WHERE transaction_type = 'debit'
GROUP BY merchant_category
ORDER BY total_cents DESC;
```

## Repository layout

```text
banking_pipeline/   Generator, validation, loading, and analytics
tests/             Integration and validation tests
.github/workflows/ Automated tests on pushes and pull requests
data/              Generated inputs (ignored)
artifacts/         Generated outputs (ignored)
```

## Design choices and limits

SQLite keeps the demo reproducible on a laptop. Validation is intentionally transparent rather than hidden behind a framework. The pipeline reads the batch into memory and is intended for portfolio-scale inputs; it is not a distributed processing or real-time banking platform. Quality checks identify malformed data, not fraud. No regulatory-compliance claim is made.

Possible extensions include incremental loading, Airflow scheduling, a dbt warehouse layer, and cloud object storage. These are future ideas, not implemented capabilities.

## Publish to your GitHub

1. Create an empty repository named `banking-data-pipeline` on GitHub. Do not initialize it with a README.
2. Upload the contents of this folder using **Add file → Upload files**, or run the commands below from this folder, replacing `YOUR_USERNAME`.

```sh
git init
git add .
git commit -m "Build banking data pipeline with validation and SQL analytics"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/banking-data-pipeline.git
git push -u origin main
```

Suggested repository description: **Python and SQL banking data pipeline with synthetic transactions, quality checks, SQLite analytics, and automated tests.**

## License

MIT. See `LICENSE`.
