# Airflow scheduling design

The [banking_daily DAG](../dags/banking_daily.py) implements this workflow for Apache Airflow 3. The core pipeline runs locally without Airflow; the DAG is included as source and is not deployed by this repository.

## Schedule and dependencies

Run daily at **02:00 UTC**, after the prior UTC day's transactions and controls are due. Use Airflow's logical data interval to identify the source partition, rather than the worker's clock. A 02:00-to-02:00 scheduler interval maps to the UTC calendar day before the interval's end date.

| Step | Responsibility | Failure behavior |
| --- | --- | --- |
| Wait for inputs | Check for a completed delivery manifest and immutable transaction and control files | Time out after one hour and alert on missing delivery |
| Validate delivery | Verify expected partition, schema, file hashes, and source version | Stop before touching the warehouse if the delivery is invalid |
| Load | Run the incremental loader against the persistent warehouse | Retry transient I/O errors; replay identical IDs without duplicating them |
| Check quality | Read inserted, replayed, and rejected counts from the run report | Block publication on ID conflicts or rejected rows until reviewed |
| Reconcile accounts | Run the reconciliation command against independent cumulative control totals through the same cutoff | Preserve exception report and block publication on exit 2 |
| Publish | Promote reports to a location keyed by date and source version | Publish only after the quality and reconciliation steps pass |

## Operations

- Start with `catchup=False`, `max_active_runs=1`, and a single-slot writer pool. Keep the local SQLite database on persistent storage with exclusive access; run the load and reconciliation where that storage is mounted. A distributed deployment should use a shared transactional database instead of worker-local SQLite.
- Save each attempt's CSV and JSON exports before another load overwrites the working files. Pass artifact locations between tasks, not transaction rows. Include the partition, input hash, row counts, and report locations in alerts.
- The current reconciliation command reads the full database. Process partitions in order and use cumulative source controls for the warehouse cutoff. Replaying an old partition after later partitions were loaded needs a separate warehouse copy or an as-of reconciliation query; comparing old controls to current totals is invalid.
- Backfill chronologically into a separate database and reconcile each cutoff before promotion. Treat changed transaction IDs as exceptions requiring an explicit correction process. A failed post-load quality gate blocks downstream publication but does not roll back the committed load.

## Configure and run

1. Make the repository root importable on the scheduler, DAG processor, and task worker (for example, mount the checkout and add it to `PYTHONPATH`). Make `dags/banking_daily.py` available in Airflow's DAG folder.
2. Set `BANKING_PIPELINE_ROOT` to the same persistent, writable path on the task worker. Create a one-slot Airflow pool named `banking_sqlite_writer`. Use one machine for SQLite access; do not place the live database on a network filesystem with unreliable locking.
3. Put `transactions.csv` and `account_controls.csv` under `incoming/YYYY-MM-DD/`. Deliver `manifest.json` **last**, after both files are complete. Do not modify a delivered version in place. The control CSV must contain cumulative account totals through that partition, not just that day's totals.
4. Before enabling the schedule, check DAG imports with `airflow dags list-import-errors`. To test a historical partition with a fresh warehouse, trigger `banking_daily` with `{"partition":"2026-01-01"}` as the run configuration. Start with a delivery whose controls match; the README exception fixture is meant to fail the quality gate.

Example manifest shape (replace the placeholders with the actual lowercase SHA-256 hashes):

```json
{
  "partition": "2026-01-01",
  "version": "v1",
  "sha256": {
    "transactions.csv": "<sha256>",
    "account_controls.csv": "<sha256>"
  }
}
```

Review `attempts/` for failed checks. Successful reports are written under `published/<partition>/<run-key>/`; consumers should wait for `_SUCCESS.json`. Task failures are visible in Airflow; external alert delivery requires configuring the deployment's notification integration.

The DAG uses the [Airflow Task SDK](https://airflow.apache.org/docs/task-sdk/stable/) for task definitions and runtime context.
