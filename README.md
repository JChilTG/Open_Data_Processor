# Bronze Ingestion for Azure Synapse

A fully parameterised, metadata-driven PySpark processor that incrementally
ingests **parquet / csv / delimited-txt** files from a landing zone into
**Delta bronze tables**. Each data feed is described by a single YAML *profile*.
Downstream, dbt (in a dedicated/serverless SQL pool) builds silver/gold from
these bronze tables.

## What it does

- **Incremental**: only files not already loaded successfully are processed
  (tracked in `control.bronze_ingestion_log`).
- **Robust typing**: every column is read as a string, sanitised, then safely
  cast with `try_cast`. Nothing throws on a bad value.
- **Quarantine, never drop**: rows that fail parsing, typing, `required` or
  `regex` checks are written to a quarantine Delta table with a per-row
  `_reasons` array and the original raw values.
- **Reliable**: optional file-readiness filter, quarantine circuit-breaker,
  idempotent Delta writes, `force_reprocess` backfill switch, and full audit
  logging.
- **Performant for large text files**: explicit schema (no inference pass),
  `multiLine=false` (files stay splittable), tunable `maxPartitionBytes`, and
  no Python UDFs.

## Layout

```
config/
  defaults.yaml                 # shared defaults, merged under every profile
  profiles/
    crm_customer.yaml           # one profile per data feed (sample)
notebooks/
  bronze_ingest.py              # the processor (import as a Synapse notebook)
  bronze_publish.py             # load bronze Delta -> dedicated SQL pool
  generate_pool_ddl.py          # emit dedicated-pool CREATE TABLE DDL from a profile
  generate_dbt_artifacts.py     # emit dbt sources.yml + staging models from profiles
  publish_metrics.py            # load run metrics + column profiles -> dedicated SQL pool
  bronze_maintenance.py         # OPTIMIZE / ZORDER / VACUUM
sql/
  control_tables.sql                  # reference DDL for the control/audit tables
  dedicated_pool_tables.sql           # pre-create DDL for the dedicated pool bronze tables
  dedicated_pool_metrics_tables.sql   # pre-create DDL for the dedicated pool metrics tables
pipeline/
  PL_Bronze_Daily.json                # Synapse pipeline: discover profiles -> ingest -> publish -> metrics
trigger/
  TR_Bronze_Daily.json                # daily schedule trigger for the pipeline
dataset/
  DS_BronzeProfilesFolder.json        # folder dataset over the profiles/ dir (profile auto-discovery)
linkedService/
  LS_ADLS_Bronze.json                 # ADLS Gen2 linked service (managed identity)
docs/
  PIPELINE_SETUP.md                   # step-by-step Synapse deployment guide
```

> Deploying into Synapse? See **[docs/PIPELINE_SETUP.md](docs/PIPELINE_SETUP.md)**
> for a full setup walkthrough (permissions, placeholders, table creation,
> parameter reference, runbook, troubleshooting).

Target runtime: **Spark 3.5, Delta Lake 3.2, Python 3.11, Scala 2.12.17,
Java 11, R 4.4.1** (open-source Delta, no Databricks). PySpark, PyYAML and
`notebookutils`/`mssparkutils` are provided by the Spark pool. Compatible with
intelligent cache and session-level packages enabled.

Lake zones referenced by profiles:
`landing/<source>/<feed>/`, `bronze/<db>/<table>/`,
`quarantine/<db>/<table>/`, and the control database tables.

## Bronze table columns

Every bronze row carries the source columns (typed per the profile schema) plus:

| column | meaning |
| --- | --- |
| `_source_file` | full path of the originating file |
| `_bronze_loaded_at_utc_ts` | load timestamp (UTC) |
| `_bronze_load_date` | load date (partition column) |
| `_bronze_profile_name` | the profile that produced the row |
| `_token_<name>` | values captured from the path via `source.path_template` (optional) |
| `_bronze_extra_cols` | JSON of unexpected source columns when `quality.schema_evolution: extra_cols_map` (optional; bronze-only, not published to the SQL pool) |
| `_bronze_change_key` | `sha2` hash over the profile's `business_fields` |
| `_bronze_batch_id`, `_bronze_run_id`, `_bronze_processor_version` | lineage |

### Schema drift (`quality.schema_evolution`)

Because the reader uses an explicit schema, source columns are reconciled
against the declared `schema` before typing:

- `extra_cols_map` (default): unexpected source columns are captured into a JSON
  `_bronze_extra_cols` column (nothing is silently dropped); columns declared in
  the profile but missing from the file are added as `null`. The column is always
  present in this mode (it is `null` when no extras are found, or when columns
  cannot be detected — e.g. headerless CSV). It is kept in the bronze Delta table
  only and is dropped on publish (via `sql_pool.drop_columns`), so it never
  reaches the dedicated SQL pool.
- `merge_schema`: Delta widens the bronze table with the new columns.
- `fail`: the run aborts (logged as `schema_drift`) when extra or missing columns
  are detected.

Drift detection compares the declared schema to the first file's header (CSV/TXT)
or the parquet schema, so it assumes a consistent layout within a batch.

For headered CSV/TXT (and parquet), columns are mapped to the schema **by name**,
not by position: a reordered or newly-inserted source column no longer silently
lands in the wrong field. Headerless CSV is still positional (no names to match).

## Adding a new feed

1. Copy `config/profiles/crm_customer.yaml` to `config/profiles/<your_feed>.yaml`.
2. Set `profile_name`, `source.path` (landing), `target.path` (bronze),
   `quarantine.path`, the `schema`, and `business_fields`.
3. Override only what differs from `config/defaults.yaml`.
4. Run the notebook with `profile_name = <your_feed>`.

### Delimited `.txt` example

`.txt` files are treated as delimited text. Set `source.format: txt` and the
separator under `source.options.sep` (e.g. `"|"` or `"\t"`).

### Path tokens

Capture parts of the file path/name into columns via `source.path_template`, a
template with `{token}` placeholders matched against each file's full path. Each
`{token}` becomes a `_token_<token>` string column in the bronze table.

```yaml
source:
  path_template: "abs/exports/data_{extract_date}/current.csv"
  # token_pattern: "[^/]+"   # optional; regex each {token} matches
```

For a file `.../abs/exports/data_january_2025/current.csv`, this adds
`_token_extract_date = "january_2025"`. Tokens flow through to the dedicated pool
and are included automatically by `generate_pool_ddl.py` (typed
`NVARCHAR(sql_pool.token_string_length)`).

## Running in Synapse

1. Import the `notebooks/*.py` files (`bronze_ingest`, `bronze_publish`,
   `publish_metrics`, `generate_pool_ddl`, `generate_dbt_artifacts`,
   `bronze_maintenance`) as Synapse notebooks (paste into a notebook, or sync
   from the repo). Mark the `PARAMETERS` cell as the **parameters cell**.
2. Ensure the Synapse workspace **managed identity** has
   `Storage Blob Data Contributor` on the landing, bronze, quarantine and
   control containers.
3. Make `config/` available to the cluster (mounted ADLS path, or set
   `config_root` to an `abfss://` path that `mssparkutils.fs.head` can read).

### Parameters

| parameter | default | purpose |
| --- | --- | --- |
| `profile_name` | `""` | **required** profile to run |
| `config_root` | `config` | folder holding `defaults.yaml` + `profiles/` |
| `batch_id` / `run_id` | auto | supplied by the pipeline; auto-generated if blank |
| `log_level` | `INFO` | `DEBUG` for verbose troubleshooting |
| `dry_run` | `false` | validate + count only, write nothing |
| `max_files_per_run` | `0` | cap files per run (0 = no cap) |
| `force_reprocess` | `false` | re-ingest matching files, ignoring the control table |

The notebook returns a metrics JSON via `mssparkutils.notebook.exit(...)` so a
pipeline can branch/alert on `status`, `quarantine_pct`, row counts, etc.

### Pipeline wiring (ForEach, one run per feed)

1. A **Lookup**/parameter supplies the list of profile names.
2. A **ForEach** iterates the list; inside it a **Notebook** activity runs
   `bronze_ingest` with `profile_name = @item()`, plus the pipeline run id as
   `batch_id`/`run_id`.
3. Set the ForEach to the desired parallelism. Keep concurrency to **one run
   per profile** to avoid double-processing the same feed.
4. Optionally branch on the returned `status` (`circuit_breaker` raises, failing
   the activity) to trigger alerts.
5. On success, run `bronze_publish` in the same loop iteration with the same
   `batch_id` to load the new rows into the dedicated SQL pool (gate it on the
   ingest activity succeeding so ingest + publish stay atomic per feed).
6. Optionally run `publish_metrics` with the same `run_id` to push that run's
   metrics + column profiles into the dedicated SQL pool for trend/anomaly use.

### Ready-made daily pipeline

`pipeline/PL_Bronze_Daily.json` + `trigger/TR_Bronze_Daily.json` (plus the
`dataset/` and `linkedService/` artifacts) implement the wiring above and
**auto-discover the feeds** — there is no hard-coded profile list. A
`Get Metadata` activity enumerates the ADLS Gen2 profiles folder, a `Filter`
keeps only `*.yaml`/`*.yml`, and a `ForEach` runs three notebook activities per
file (each gated on the previous succeeding), passing the pipeline run id as
both `batch_id` and `run_id`:

```
ListProfiles          (Get Metadata: childItems of profilesFolder)
FilterYaml            (keep File + .yaml/.yml, optionally narrowed by profileFilter)
ValidateProfilesFound (Fail fast if 0 profiles discovered, unless disabled)
ForEach(FilterYaml.output.value)        (parallel, batchCount 4)
  IngestStage         If(runIngest)         -> bronze_ingest    (profile_name = item().name minus extension)
  PublishStage        If(runPublish)        -> bronze_publish   (Succeeded(IngestStage))
  PublishMetricsStage If(runPublishMetrics) -> publish_metrics  (Succeeded(PublishStage))
```

The profile name is the file name with the extension stripped, so
`crm_customer.yaml` -> profile `crm_customer` (it must match the notebook's
`profile_name` lookup). Because the bronze write and publishes are idempotent by
`batch_id`, the built-in activity `retry` is safe — a retried run replaces its
own rows instead of duplicating. `Publish`/`PublishMetrics` self-skip when
`sql_pool.enabled` / `metrics_publish.enabled` are false.

**Import & configure (Synapse Studio → Integrate / Manage, or Git-sync):**

1. Import the notebooks (`bronze_ingest`, `bronze_publish`, `publish_metrics`)
   so their names match the `NotebookReference`s in the pipeline.
2. In `linkedService/LS_ADLS_Bronze.json`, set the `url` to your ADLS Gen2
   account (`https://<account>.dfs.core.windows.net`) and grant the workspace
   managed identity **Storage Blob Data Reader/Contributor** on it.
3. In `PL_Bronze_Daily.json`, replace `SET_YOUR_SPARK_POOL_NAME` (3 activities)
   with your Spark pool, and set the defaults for:
   - `profilesFileSystem` / `profilesFolderPath` — the container + path of the
     `profiles/` folder the Get Metadata activity scans (default
     `config` + `bronze/profiles`).
   - `configRoot` — the ADLS path holding `defaults.yaml` + `profiles/` that the
     notebooks read (default `abfss://config@<account>.dfs.core.windows.net/bronze`).
4. In `TR_Bronze_Daily.json`, adjust `startTime` / `schedule` (default 02:00 UTC)
   and flip `runtimeState` to `Started` (or start the trigger in the UI) to go live.

Drop a new `*.yaml` into the profiles folder and the next run picks it up — no
pipeline edit needed. Different profiles run in parallel (`batchCount: 6`); each
profile appears once per run, preserving the one-run-per-feed guarantee.

**Running a subset of feeds:** the `profileFilter` array parameter (default
`[]` = all discovered profiles) restricts the run to specific feeds. The
`FilterYaml` step keeps a discovered file only when `profileFilter` is empty
**or** it contains that file's profile name (extension stripped). So an ad-hoc
run or a second trigger can pass e.g. `["crm_customer", "sales_orders"]` to
process just those, while the daily trigger leaves it empty to run everything.

**Running a subset of stages:** the booleans `runIngest`, `runPublish`,
`runPublishMetrics` (all default `true`) each gate one stage via an
`IfCondition`. The stages are still chained on `Succeeded`, so skipping an
earlier one doesn't break a later one — set `runIngest=false`,
`runPublish=true` to (re)publish an already-ingested batch, or
`runPublishMetrics=true` with the others `false` to just push metrics.
Combine with `profileFilter` to, say, re-publish only `crm_customer`.

**Targeting a prior batch:** publish/metrics filter by `batch_id`/`run_id`,
which default to the current `@pipeline().RunId`. To re-publish or re-push
metrics for an *earlier* run, set `batchIdOverride` to that run's id (the
`_bronze_batch_id`/`run_id` stamped when it was ingested). Leave it empty
(the default) for normal runs. Typical reprocess: `runIngest=false`,
`runPublish=true`, `batchIdOverride="<original-run-id>"`,
`profileFilter=["crm_customer"]`.

**Hardening built into the pipeline:**

- **No overlapping runs** — `concurrency: 1` on the pipeline queues a new run
  if a previous one is still going (e.g. a long backfill overrunning the daily
  schedule), so the same feed is never processed by two runs at once.
- **Fail fast on empty discovery** — `ValidateProfilesFound` fails the run with
  a descriptive error if zero profiles match (wrong `profilesFolderPath`,
  missing linked-service permissions, or a `profileFilter` typo) instead of
  going green having loaded nothing. Set `failIfNoProfiles=false` to allow
  empty runs (e.g. an on-demand run scoped to a feed that may not exist yet).
- **Spark pool capacity** — `ForEach` runs `batchCount: 4` profiles in
  parallel, each in its own notebook session. Raise/lower it to match your
  Spark pool's node/core quota; too high causes session queueing or failures.
  (`batchCount` must be a literal, so it's edited in the JSON, not a parameter.)
- **One bad feed doesn't sink the rest** — `ForEach` is non-sequential, so a
  failing profile fails only its own iteration; other profiles still complete
  and the overall run is marked Failed for alerting.
- **Safe retries** — each notebook activity has `retry: 1`; combined with the
  idempotent (delete-by-`batch_id`) writes, a retry replaces its own rows
  rather than duplicating.
- **Monitorable** — each notebook activity carries a `profile` user property,
  so the Synapse monitoring grid shows which feed each activity belongs to.

A couple of things to keep aligned yourself: `profilesFileSystem`/
`profilesFolderPath` (what discovery scans) must point at the same folder as
`configRoot` (what the notebooks read), and `profileFilter` matching is
case-sensitive.

## bronze -> dedicated SQL pool publish

Bronze is loaded **physically** into a dedicated SQL pool table (no external
tables, no serverless) using the **Azure Synapse Dedicated SQL Pool Connector
for Apache Spark**. The connector bulk-loads via the `COPY` command with
managed-identity ADLS staging.

`notebooks/bronze_publish.py` reads the bronze Delta table (one ingest batch, or
the whole table) and writes it to the dedicated pool, driven by the `sql_pool`
block in each profile:

```yaml
sql_pool:
  enabled: true
  database: edw                 # dedicated pool database (same workspace = auto-detected)
  schema: bronze
  table: crm_customer
  temp_folder: "abfss://staging@<acct>.dfs.core.windows.net/synapse_tmp"   # COPY staging
  mode: append                  # append | overwrite
  drop_columns: ["_bronze_run_id", "_bronze_processor_version"]
  rename: {}                    # pool columns mirror the bronze _bronze_* names
```

Publish parameters: `profile_name`, `config_root`, `batch_id` (publish that
ingest batch), `publish_all` (load the whole table), `log_level`.

For retry-safe publishing, set `sql_pool.idempotent: true`: before appending,
the notebook deletes any rows already present for the `batch_id` in the pool
table (a no-op if the table doesn't exist yet). This requires `sql_pool.server`
(the SQL endpoint, e.g. `<workspace>.sql.azuresynapse.net`) and the workspace
managed identity to have `DELETE` rights, because the delete runs over JDBC with
an AAD token. `metrics_publish.idempotent` does the same for the metrics tables
by `run_id`/`batch_id`.

Pre-create the destination table with a deliberate `DISTRIBUTION` + columnstore
index — see [sql/dedicated_pool_tables.sql](sql/dedicated_pool_tables.sql) for an
example. Rather than hand-write it per feed, run `notebooks/generate_pool_ddl.py`
with `profile_name` to emit a `CREATE TABLE` whose columns/order match the
published DataFrame automatically. It reads the profile `schema` (honouring
per-column `sql_type` / `sql_length`) and the `sql_pool` settings
(`distribution`, `hash_column`, `index`, `default_string_length`), prints the
DDL, and optionally writes it to `output_path`.

Requirements: the workspace **managed identity** must be a user in the dedicated
pool with load rights and have `Storage Blob Data Contributor` on the
`temp_folder` container. For a pool in a *different* workspace, set
`sql_pool.server` to `<workspace>.sql.azuresynapse.net`.

dbt then builds silver/gold from these dedicated-pool `bronze.*` tables. The
`_bronze_change_key` column supports cheap incremental/change-detection models.

## dbt artifact generation

`notebooks/generate_dbt_artifacts.py` generates dbt files from the same profiles:

- `models/bronze/sources.yml` with one source table per profile.
- `models/staging/stg_<profile_name>.sql` models that select the published
  dedicated-pool columns from `source(...)`.

It uses the dedicated-pool shape, so the generated dbt column list matches
`bronze_publish.py` and `generate_pool_ddl.py` exactly: source schema columns,
`_source_file`, optional `_token_*` columns, then `_bronze_*` metadata columns,
after applying `sql_pool.drop_columns` / `sql_pool.rename`.

Run it for all profiles:

```yaml
profile_name: ""                 # blank = all profiles
config_root: "abfss://.../config"
output_root: "abfss://.../dbt"    # writes models/bronze/sources.yml + models/staging/*.sql
```

Or run for a single profile by setting `profile_name`. If `output_root` is
blank, the notebook only logs/returns the generated YAML and SQL via
`notebook.exit`.

Default dbt generation settings live in `config/defaults.yaml` under `dbt`:

```yaml
dbt:
  enabled: true
  source_name: bronze
  staging_model_prefix: stg_
  staging_materialized: view
  include_column_data_types: true
  include_not_null_tests: true
```

## Run metrics & per-load profiling

Every run records two things automatically (Delta control tables, created on
first run):

- `control.bronze_run_log` — one row per run: files discovered/new/processed,
  rows read/loaded/quarantined, quarantine %, status, duration, etc.
- `control.bronze_column_profile` — one row per loaded column per run with the
  per-load profiling metrics used for trend lines and anomaly detection:
  `row_count`, `null_count`, `null_pct`, `distinct_count` (`distinct_is_approx`
  flags HyperLogLog vs exact), and `min_value` / `max_value` (stored as strings).
  The whole profile is computed in a single aggregation pass over the cached
  `good` frame (see the caching note below).

Profiling is configured under `profiling` in `config/defaults.yaml`:

```yaml
profiling:
  enabled: true
  approx_distinct: true     # approx_count_distinct (cheap) vs exact countDistinct
  include_min_max: true
  columns: null             # null = all schema columns; or a list of names
```

### Publishing metrics to the dedicated SQL pool

`notebooks/publish_metrics.py` pushes both control tables into matching
dedicated-pool tables (reusing the `sql_pool` connection), so dbt / Power BI can
trend volumes, quarantine rates, null %, distinct counts and min/max over time
and alert on anomalies. Pre-create the destination tables with
`sql/dedicated_pool_metrics_tables.sql`. Enable and configure under
`metrics_publish`:

```yaml
metrics_publish:
  enabled: true
  database: null            # null = reuse sql_pool.database
  schema: operations        # pool schema for the metrics tables
  run_log_table: bronze_run_log
  column_profile_table: bronze_column_profile
  mode: append
```

Run it in the same `ForEach` iteration after `bronze_publish`, passing the same
`run_id` (or `batch_id`) so only that run's metrics are published; set
`publish_all=True` for a one-off backfill.

## Maintenance

Schedule `notebooks/bronze_maintenance.py` per profile to run `OPTIMIZE`
(optional `ZORDER`) and `VACUUM`. Set `vacuum_retain_hours` deliberately — it
bounds the Delta time-travel window. Delta refuses `VACUUM RETAIN < 168 HOURS`
(it can break time travel / concurrent readers); the notebook clamps to 168h
with a warning unless you pass `allow_low_retention=true`, in which case it
disables the retention check just for that run.

## Performance notes (large `.txt`)

- Keep `multiLine: false` so big files stay splittable across tasks.
- Explicit schema avoids an extra inference pass.
- Lower `spark.sql.files.maxPartitionBytes` to fan a single large file across
  more tasks; size the Spark pool accordingly.
- `gzip` is **not splittable** (one core reads the whole file). Prefer
  uncompressed or `snappy`/`bzip2`, or split large files before landing.
- The sanitised input and the typed `good` / `quarantine` frames are
  `persist()`ed (MEMORY_AND_DISK) before counting, so the file read,
  sanitisation and typing run **once** and are reused by the row counts, the
  Delta writes and the profiling pass — avoiding repeated full-lineage scans on
  large inputs. The frames are unpersisted in a `finally` on every exit path.

## Notes

- Optimized write and auto-compact are enabled via OSS Delta **table
  properties** (`delta.autoOptimize.optimizeWrite` / `delta.autoOptimize.autoCompact`)
  set on the bronze table in `write_bronze`. We deliberately avoid a
  session-level optimize-write conf because that key name has varied across
  Synapse runtime versions; the table properties are respected by Delta 3.2
  regardless. Toggle per profile via `target.optimize_write` / `target.auto_compact`.
- The notebooks rely on Spark 3.5 / Delta 3.2 features: `try_cast`, the
  higher-order `filter` function, and `mssparkutils` from the pool.
- **Idempotent bronze writes** (`target.idempotent_writes`, default on): before
  appending, rows already present for the current `batch_id` are deleted, so a
  pipeline retry of the same batch never duplicates data and never silently
  skips a write.
- **Retry / dead-letter** (`quality.max_attempts`, default 3): a file that keeps
  failing is parked as `dead_letter` in `control.bronze_ingestion_log` after
  `max_attempts` and is then skipped by the incremental filter instead of being
  retried forever. `attempt_count` records how many tries each file took.
- **Date/time parsing**: `spark.sql.legacy.timeParserPolicy` is set to
  `CORRECTED` so unparseable dates/times return null (and get quarantined as
  `:cast`) rather than throwing and failing the whole batch.
- **Profile validation**: profiles are validated at load — duplicate or reserved
  column names, unknown types, invalid regexes, partition columns that don't
  exist, and a `HASH` distribution whose `hash_column` is missing/dropped all
  fail fast with a clear message.
