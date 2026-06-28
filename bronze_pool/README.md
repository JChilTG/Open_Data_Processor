# Bronze loader: Delta (ADLS Gen2) -> `edw` dedicated SQL pool

A single Synapse PySpark notebook ([notebooks/brz_delta_to_sqlpool.ipynb](notebooks/brz_delta_to_sqlpool.ipynb))
that incrementally loads every Delta table found under one or more ADLS Gen2
roots into the `edw` dedicated SQL pool's `brz` schema, ready for dbt.

- Synapse only (Spark 3.5 / Python 3.11). No Databricks components.
- Change detection via **Delta Change Data Feed (CDF)**.
- **Append-only** bronze: every CDF change row is kept as an immutable change log; dbt de-dupes / materializes current state downstream.
- **Workspace managed identity** auth only — no account keys, no SQL passwords.
- Every `brz` table is created `WITH (DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX)`.
- Business columns are all `NVARCHAR` (no type inference); a few typed metadata columns are added for correctness.
- **Idempotent / exactly-once per run**, **resilient** (one bad table never aborts the run), and **thoroughly logged** to stdout and to the `edw.operations` schema.

## Repository layout

| Path | Purpose |
| --- | --- |
| [notebooks/brz_delta_to_sqlpool.ipynb](notebooks/brz_delta_to_sqlpool.ipynb) | The loader notebook (pipeline-runnable + standalone). |
| [sql/00_setup.sql](sql/00_setup.sql) | One-time: schemas + `operations` control/log tables (no permission changes). |
| [sql/01_maintenance.sql](sql/01_maintenance.sql) | Periodic columnstore rebuild + stats refresh (run nightly/weekly). |
| [config/tables.json](config/tables.json) | Optional include/exclude, change-type filter, per-table string length. |

## How it works

```
discover Delta tables (folders with _delta_log)
  -> read watermark from edw.operations.load_control
     -> initial (no watermark)  : full snapshot  -> stage -> TRUNCATE+INSERT target
        incremental (watermark) : CDF start..end -> stage -> DELETE range + INSERT target
        up to date              : SKIP
  -> advance watermark + write audit row to edw.operations.load_log (per table)
  -> continue to next table even if one fails
  -> write run summary + notebook.exit(json)
```

### Idempotency
The Spark bulk load lands in a per-table staging table (`brz.<table>__stage`,
always overwritten). The target is then updated inside **one JDBC transaction**:

- initial load: `TRUNCATE target; INSERT INTO target SELECT ... FROM stage`
- incremental: `DELETE FROM target WHERE _commit_version BETWEEN start AND end; INSERT ...`

and the watermark in `operations.load_control` is advanced **only on commit**.
If a run dies before commit, the watermark is unchanged and the next run safely
reprocesses the identical version range (the `DELETE` makes it a no-op-on-retry).
Re-running with no new source commits is a clean `SKIP`.

### Columns added to every bronze table
`_change_type` (NVARCHAR), `_commit_version` (BIGINT), `_commit_timestamp`
(DATETIME2), `_load_run_id` (NVARCHAR), `_loaded_utc` (DATETIME2),
`_source_path` (NVARCHAR). `_commit_version` stays a BIGINT so the idempotent
range delete is correct.

## Setup (once)

1. **Run the setup SQL** against the `edw` pool: execute [sql/00_setup.sql](sql/00_setup.sql).
   It creates the `brz` and `operations` schemas and the
   `operations.load_control` / `operations.load_log` tables. It makes **no
   permission changes** - the workspace managed identity is assumed to already
   have the required database access.

2. **Ensure storage access** for the workspace managed identity (typically
   already in place):
   - read on the **source** container(s).
   - `Storage Blob Data Contributor` on the **staging** container (`staging_path`).

3. **Enable Change Data Feed on source Delta tables.** CDF only captures changes
   made after it is enabled, so do this before/at first load. Per table:

   ```python
   spark.sql("""
       ALTER TABLE delta.`abfss://<container>@<acct>.dfs.core.windows.net/<path>`
       SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
   """)
   ```

   or set the session default so all new Delta tables get it:

   ```python
   spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
   ```

   (The config key name is historical; open-source Delta in the Synapse runtime
   honours it — no Databricks dependency.)

4. **Import the notebook** into your Synapse workspace and attach it to the
   Spark 3.5 (GA) pool. The dedicated-pool connector
   (`com.microsoft.spark.sqlanalytics`) and the Microsoft SQL JDBC driver ship
   by default with the Synapse 3.5 runtime - nothing extra to install.

## Parameters

Set in the notebook's **Parameters** cell (defaults let it run standalone) or
override from a pipeline Notebook activity.

| Parameter | Default | Notes |
| --- | --- | --- |
| `sql_endpoint` | `""` | **Required.** e.g. `myws.sql.azuresynapse.net`. |
| `database` | `edw` | Dedicated SQL pool / database. |
| `target_schema` | `brz` | Schema for bronze tables. |
| `ops_schema` | `operations` | Schema for control + log tables. |
| `source_paths` | `""` | **Required.** Comma-separated `abfss://` roots (or a list) to scan. |
| `staging_path` | `""` | **Required.** `abfss://` folder for connector COPY staging. |
| `include_tables` | `""` | Process ONLY these bronze table names (comma-separated or list). Blank = all discovered. |
| `exclude_tables` | `""` | Skip these bronze table names (comma-separated or list). |
| `string_len` | `4000` | Default `NVARCHAR` length for business columns. |
| `max_workers` | `4` | Parallel tables; keep within SQL pool concurrency slots. |
| `token_audience` | `DW` | AAD audience for the pool token. Try `Synapse` if `DW` is rejected. |
| `config_path` | `""` | Optional `abfss://` path to `config/tables.json`. |
| `fail_on_error` | `False` | If `True`, raise at the end when any table failed. |

## Running

- **Standalone (testing):** open the notebook, fill the required parameters, run all.
- **Pipeline (scheduled):** add a Notebook activity pointing at this notebook,
  pass the parameters above, and attach a schedule trigger. The notebook returns
  a JSON summary via `mssparkutils.notebook.exit(...)` so downstream activities
  can branch on `succeeded` / `failed` / `skipped`.

## Processing only a specific list of tables

Set `include_tables` to a comma-separated list (or leave blank for everything).
The names are the **bronze table names**, which are the source folder names with
any non-alphanumeric characters replaced by `_` (e.g. folder `dim-customer` ->
`dim_customer`). Use `exclude_tables` to skip a few instead.

- **Standalone notebook:** edit the Parameters cell, then run all:

```python
include_tables = "customers,orders,dim_product"
```

- **Synapse pipeline:** in the Notebook activity's Base parameters, add a string
  parameter named `include_tables` with value `customers,orders,dim_product`.

- **Config file alternative** (`config/tables.json`, when `config_path` is set) -
  parameter values and the file are unioned:

```json
{ "include": ["customers", "orders", "dim_product"] }
```

Any name in `include_tables` that isn't found under `source_paths` is logged as a
warning, and the log line `Processing N table(s): ...` shows exactly what ran.

## Monitoring

```sql
-- latest run per table
SELECT * FROM edw.operations.load_log ORDER BY started_utc DESC;

-- current watermarks
SELECT * FROM edw.operations.load_control ORDER BY table_name;

-- failures only
SELECT * FROM edw.operations.load_log WHERE status = 'FAILED' ORDER BY ended_utc DESC;
```

## Backfill / re-seed a single table

Force a full reload on the next run by clearing its watermark (the next run does
a snapshot + `TRUNCATE`+`INSERT`, so no duplicates):

```sql
DELETE FROM edw.operations.load_control WHERE table_name = '<table>';
-- optional hard reset:
-- DROP TABLE edw.brz.[<table>];
-- DROP TABLE edw.brz.[<table>__stage];
```

## Common issues (and how this design handles them)

Things that bite people with Synapse dedicated pools, and what we did about each.

### Already handled in the code
- **Trickle inserts into columnstore degrade over time.** Append-only + small
  CDF batches create many tiny open row-groups. Run [sql/01_maintenance.sql](sql/01_maintenance.sql)
  on a schedule to `REBUILD` and `UPDATE STATISTICS`. Larger, less frequent runs
  also help (fewer, bigger batches per table).
- **`mssparkutils` vs `notebookutils`.** Newer runtimes renamed it; the notebook
  aliases whichever exists, so it runs on both.
- **Identifiers with odd characters.** Source column/table names are
  bracket-quoted and `]`-escaped (`ident()`), and bronze table names are
  sanitized, so unusual names don't break DDL or cause injection.
- **Partial/failed run leaving duplicates.** The watermark advances only inside
  the committed swap transaction; a re-run reprocesses the same version range,
  which the range `DELETE` (incremental) or `TRUNCATE` (initial) makes safe.
- **One bad table aborting everything.** Each table is isolated in try/except;
  failures are logged to `operations.load_log` and the run continues.
- **CDF history vacuumed past the needed version.** Detected on read; the table
  automatically falls back to a full reload and resets its watermark.
- **CDF not enabled yet.** First run is always a full snapshot, so you can enable
  CDF and load on day one; only subsequent incrementals need CDF.

### Things you should check / tune
- **Runtime: Synapse Spark 3.5 (GA).** Components: Spark 3.5, Python 3.11, Delta
  Lake 3.2, Java 17, Scala 2.12.18. The `com.microsoft.spark.sqlanalytics`
  connector and MS SQL JDBC driver ship by default with this runtime, so no
  packages need to be attached. Delta 3.2 supports Change Data Feed natively.
- **Token audience.** `token_audience` defaults to `DW`. If `getToken` rejects
  it, set it to `Synapse`. This is the single most likely first-run failure.
- **Concurrency slots.** `max_workers` parallel loads each consume dedicated-pool
  concurrency/memory slots. On small SKUs (DW100c-DW200c) keep `max_workers` low
  (2-4) or loads will queue/fail. Scale the pool up for big backfills.
- **`string_len` vs real data.** A value longer than `string_len` fails that
  table's COPY (isolated + logged). Raise `string_len` globally or per table in
  `config/tables.json`. Keep it bounded (no `MAX`) so the uniform CCI holds.
- **Large initial loads.** The first load reads the full snapshot and does an
  `INSERT ... SELECT` inside a transaction. For very large tables this is slower
  than CTAS; run the first load on a scaled-up pool, then let incrementals take
  over. (We use a pre-created table to guarantee distribution/index, which CTAS
  would otherwise control.)
- **Recursive ADLS discovery cost.** Scanning very large/deep containers via
  `fs.ls` can be slow. Point `source_paths` at the specific Delta roots, or use
  `include_tables` to limit work.
- **Case-insensitive collation.** The pool is case-insensitive by default, so two
  source columns differing only in case (e.g. `Id` and `id`) would collide. Rare
  in practice; surfaces as a per-table failure in the log if it happens.
- **Workspace identity in pipeline vs interactive runs.** Interactive notebook
  runs authenticate as *you*; pipeline runs authenticate as the *workspace MI*.
  Make sure the MI (not just your user) has the needed DB + storage access.

## Notes / limitations

- All business values land as strings; casting/typing and current-state
  modelling are dbt's responsibility in the next layer.
- CCI requires bounded `NVARCHAR` (no `MAX`), which is why `string_len` must stay
  within the columnstore limit.
