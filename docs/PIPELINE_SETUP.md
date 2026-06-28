# Bronze pipeline — Synapse setup guide

End-to-end instructions for deploying the daily bronze ingestion pipeline and
its supporting artifacts (linked service, dataset, notebooks, trigger) into an
Azure Synapse Analytics workspace, including every placeholder you must replace.

The pipeline auto-discovers one `*.yaml` profile per feed from an ADLS Gen2
folder and, for each, runs `bronze_ingest -> bronze_publish -> publish_metrics`.

```
ListProfiles          Get Metadata: childItems of the profiles folder
FilterYaml            keep *.yaml / *.yml (optionally narrowed by profileFilter)
ValidateProfilesFound fail fast if nothing matched
ForEach (batchCount 4, parallel per feed)
  IngestStage         If(runIngest)         -> bronze_ingest
  PublishStage        If(runPublish)        -> bronze_publish    (Succeeded(Ingest))
  PublishMetricsStage If(runPublishMetrics) -> publish_metrics   (Succeeded(Publish))
```

---

## 1. Prerequisites

| Component | Requirement |
|---|---|
| Synapse workspace | With a Git or Live publish target. |
| Apache Spark pool | **Spark 3.5** (Delta Lake 3.2). Enable autoscale sized for `batchCount` parallel notebook sessions. |
| Dedicated SQL pool | Only needed if publishing bronze / metrics to the pool. |
| ADLS Gen2 | A storage account + containers for **config**, **landing** data, **bronze** Delta tables, and a **staging** folder for the COPY load. |
| Workspace managed identity (MI) | Used for all storage and SQL-pool auth. No keys/secrets in config. |
| Packages | `PyYAML`, OSS `delta`, `notebookutils`, and the Synapse **Dedicated SQL Pool Connector** (`com.microsoft.spark.sqlanalytics`) — all preinstalled on Synapse Spark 3.5 pools. |

---

## 2. Identity & permissions

Grant the **Synapse workspace managed identity** (named after the workspace):

1. **ADLS Gen2** — role **Storage Blob Data Contributor** on the storage
   account (or at least each container the pipeline touches: config, landing,
   bronze, staging). The Get Metadata discovery, the Spark reads/writes, and the
   COPY staging all authenticate as the MI.
2. **Dedicated SQL pool** (only if `sql_pool.enabled` / `metrics_publish.enabled`)
   — add the MI as a database user and grant load + (for idempotent publish)
   delete rights. Run in the **dedicated pool**:

   ```sql
   CREATE USER [<your-workspace-name>] FROM EXTERNAL PROVIDER;
   GRANT INSERT, ADMINISTER DATABASE BULK OPERATIONS TO [<your-workspace-name>];
   -- needed only if sql_pool.idempotent / metrics_publish.idempotent = true:
   GRANT DELETE, SELECT TO [<your-workspace-name>];
   -- if you let the connector auto-create tables instead of pre-creating them:
   GRANT CREATE TABLE TO [<your-workspace-name>];
   ```

   For **cross-workspace** pools also set `sql_pool.server` in config and ensure
   the MI is a user on that remote pool.

---

## 3. ADLS Gen2 layout

Pick your paths and keep them consistent across config and the pipeline params.
A typical layout (container names are examples):

```
config   (container)
  bronze/
    defaults.yaml
    profiles/
      crm_customer.yaml
      sales_orders.yaml
landing  (container)        # raw source files (per-profile source.path)
bronze   (container)        # Delta bronze tables (per-profile target.path)
staging  (container)
  bronze_copy/              # sql_pool.temp_folder — COPY staging, safe to purge
```

- **`configRoot`** (notebooks read this) = the folder holding `defaults.yaml`
  + `profiles/`, e.g. `abfss://config@<account>.dfs.core.windows.net/bronze`.
- **`profilesFileSystem` / `profilesFolderPath`** (Get Metadata scans this) must
  point at the **same** `profiles/` folder, expressed as container + path, e.g.
  `config` + `bronze/profiles`.

Upload `config/defaults.yaml` and `config/profiles/*.yaml` from this repo to the
config location above.

---

## 4. Deploy the artifacts

Import via Synapse Studio (Manage / Integrate hubs) or, if Git-connected, copy
the JSON into the workspace repo under the matching root folders
(`linkedService/`, `dataset/`, `pipeline/`, `trigger/`) and publish.

### 4.1 Linked service — `linkedService/LS_ADLS_Bronze.json`
- Replace **`SET_YOUR_ACCOUNT`** in `url` with your storage account:
  `https://<account>.dfs.core.windows.net`.
- Auth is workspace MI (no edit needed). Test connection.

### 4.2 Dataset — `dataset/DS_BronzeProfilesFolder.json`
- No edits required; it's parameterized (`fileSystem`, `folderPath`) and bound
  to `LS_ADLS_Bronze`. It's a folder-level Binary dataset used only by
  Get Metadata to list the profiles folder.

### 4.3 Notebooks
Import these `.py` files as Synapse notebooks. **The notebook names must match
the references in the pipeline exactly:**

| Notebook (name in workspace) | File | Used by pipeline |
|---|---|---|
| `bronze_ingest` | `notebooks/bronze_ingest.py` | IngestStage |
| `bronze_publish` | `notebooks/bronze_publish.py` | PublishStage |
| `publish_metrics` | `notebooks/publish_metrics.py` | PublishMetricsStage |
| `generate_pool_ddl` | `notebooks/generate_pool_ddl.py` | manual (DDL gen) |
| `generate_dbt_artifacts` | `notebooks/generate_dbt_artifacts.py` | manual (dbt gen) |
| `bronze_maintenance` | `notebooks/bronze_maintenance.py` | manual / scheduled OPTIMIZE+VACUUM |

For each notebook: **attach the Spark pool** and **mark the parameters cell**
(the `PARAMETERS` block near the top) as the parameters cell so the pipeline can
inject values.

### 4.4 Pipeline — `pipeline/PL_Bronze_Daily.json`
Replace these hardcoded placeholders:

| Placeholder / setting | Where | Set to |
|---|---|---|
| `SET_YOUR_SPARK_POOL_NAME` | `sparkPool.referenceName` in **all 3** notebook activities | Your Spark pool name |
| `SET_YOUR_ACCOUNT` | `configRoot` default | Your storage account |
| `profilesFileSystem` default (`config`) | parameters | Container holding `profiles/` |
| `profilesFolderPath` default (`bronze/profiles`) | parameters | Path to `profiles/` inside that container |
| `batchCount` (`4`) | `ForEachProfile.typeProperties` | Feeds to run in parallel; match Spark pool capacity (literal, not a parameter) |

### 4.5 Trigger — `trigger/TR_Bronze_Daily.json`
- Adjust `recurrence.startTime`, `schedule` (default **02:00 UTC**) and
  `timeZone`.
- Flip **`runtimeState`** from `Stopped` to `Started` (or start it from the UI)
  when ready to go live.
- The `parameters` block sends the defaults; override here for a second schedule
  (e.g. a publish-only trigger).

---

## 5. Create the tables

### 5.1 Spark / Delta control + bronze tables — automatic
`bronze_ingest` issues `CREATE DATABASE IF NOT EXISTS` and creates the control
and bronze Delta tables on first run. `sql/control_tables.sql` is **reference
DDL** only; you don't need to run it.

### 5.2 Dedicated SQL pool tables — pre-create (recommended)
The connector loads into existing pool tables with your chosen
DISTRIBUTION + index. Pre-create them so distribution/indexing is intentional:

1. Generate DDL that matches the published column shape: run the
   `generate_pool_ddl` notebook with `profile_name` set; it returns
   `CREATE TABLE` statements (or writes them if configured). `sql/dedicated_pool_tables.sql`
   is a worked example for `crm_customer`.
2. Run that DDL in the **dedicated SQL pool** (`sql_pool.schema`, default `bronze`).
3. For metrics, run `sql/dedicated_pool_metrics_tables.sql` in the dedicated pool
   (creates the `operations` schema + `bronze_run_log` / `bronze_column_profile`).

> Note: `_bronze_extra_cols` is intentionally **not** published to the pool, and
> `NVARCHAR(MAX)` is never used — string columns are bounded by
> `sql_pool.max_string_length` (4000).

---

## 6. Configure the profiles (per feed)

Each `profiles/<name>.yaml` overrides `defaults.yaml`. See
`config/profiles/crm_customer.yaml` for a complete annotated example (it also
sets a top-level `profile_name`, `business_fields` for the `_change_key` hash,
and a `quarantine.path`). Minimum to wire up publishing for a feed (omit the
`sql_pool` / `metrics_publish` blocks to keep a feed bronze-only):

```yaml
profile_name: crm_customer
source:
  path: "abfss://landing@<account>.dfs.core.windows.net/crm/customer/"
  format: csv
target:
  table: crm_customer
  path: "abfss://bronze@<account>.dfs.core.windows.net/crm_customer/"
quarantine:
  path: "abfss://quarantine@<account>.dfs.core.windows.net/crm_customer/"
schema:
  - {name: customer_id, type: long}
  - {name: full_name,   type: string, sql_length: 200}
  # ...
sql_pool:
  enabled: true
  database: <dedicated_pool_db>
  schema: bronze
  temp_folder: "abfss://staging@<account>.dfs.core.windows.net/bronze_copy/"
  distribution: HASH          # or ROUND_ROBIN / REPLICATE
  hash_column: customer_id    # required when distribution = HASH
  idempotent: true            # needs sql_pool.server (or same-workspace) + MI DELETE
metrics_publish:
  enabled: true
  # reuses sql_pool.database + temp_folder; schema defaults to "operations"
```

Key cross-checks:
- `sql_pool.temp_folder` is **required** for publishing (the COPY staging path).
- `sql_pool.database` (and `metrics_publish.database`, else it reuses it) must
  be set when publishing.
- For same-workspace pools leave `sql_pool.server: null`; set it only for
  cross-workspace or when using `idempotent` pre-delete over JDBC.

---

## 7. Parameter reference

### Pipeline `PL_Bronze_Daily`
| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `runIngest` | bool | `true` | Run the ingest stage. |
| `runPublish` | bool | `true` | Run the bronze->pool publish stage. |
| `runPublishMetrics` | bool | `true` | Run the metrics publish stage. |
| `batchIdOverride` | string | `""` | Empty = use `@pipeline().RunId`; set to a prior run id to (re)publish that batch. |
| `profileFilter` | array | `[]` | Empty = all discovered profiles; else only the named ones. |
| `failIfNoProfiles` | bool | `true` | Fail the run if 0 profiles match (catches bad paths/filters). |
| `profilesFileSystem` | string | `config` | Container of the profiles folder (discovery). |
| `profilesFolderPath` | string | `bronze/profiles` | Path of the profiles folder (discovery). |
| `configRoot` | string | `abfss://config@SET_YOUR_ACCOUNT...` | Folder with `defaults.yaml` + `profiles/` (notebook reads). |
| `logLevel` | string | `INFO` | DEBUG / INFO / WARNING / ERROR. |

### Notebook parameters injected by the pipeline
| Notebook | Parameters sent |
|---|---|
| `bronze_ingest` | `profile_name`, `config_root`, `batch_id`, `run_id`, `log_level` |
| `bronze_publish` | `profile_name`, `config_root`, `batch_id`, `log_level` |
| `publish_metrics` | `profile_name`, `config_root`, `run_id`, `log_level` |

Other notebook parameters (not set by the pipeline, available for manual runs):
`dry_run`, `max_files_per_run`, `force_reprocess` (ingest); `publish_all`
(publish / metrics).

---

## 8. First run & validation

1. **Manual smoke test (one feed):** trigger `PL_Bronze_Daily` with
   `profileFilter=["crm_customer"]`. Or run `bronze_ingest` directly with
   `dry_run=true` to validate config + counts without writing.
2. Check the **monitoring grid** — each activity shows a `profile` user property.
3. Verify the bronze Delta table and the `control.bronze_ingestion_log` /
   `control.bronze_run_log` tables in Spark.
4. If publishing, verify rows landed in the dedicated pool table and in
   `operations.bronze_run_log` / `operations.bronze_column_profile`.
5. **Go live:** start `TR_Bronze_Daily`.

---

## 9. Operational runbook

| Goal | How |
|---|---|
| Run all feeds | Default run (no params). |
| Run specific feeds | `profileFilter=["crm_customer","sales_orders"]`. |
| Run only some stages | Set `runIngest` / `runPublish` / `runPublishMetrics`. |
| Re-publish a prior batch | `runIngest=false`, `runPublish=true`, `batchIdOverride="<original-run-id>"`, optionally `profileFilter=[...]`. |
| Re-ingest matching files | Run `bronze_ingest` with `force_reprocess=true`. |
| Add a new feed | Drop a new `*.yaml` into the profiles folder — next run auto-discovers it. No pipeline edit. |
| Add more parallelism | Raise `ForEach.batchCount` (literal in the pipeline JSON) to match Spark pool capacity. |
| Maintain Delta tables | Run `bronze_maintenance` (OPTIMIZE + VACUUM; guards `RETAIN < 168h`). |
| Generate pool DDL / dbt | Run `generate_pool_ddl` / `generate_dbt_artifacts` with `profile_name`. |

---

## 10. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Run fails at `ValidateProfilesFound` (`NO_PROFILES_DISCOVERED`) | Wrong `profilesFileSystem`/`profilesFolderPath`, MI lacks ADLS read, or `profileFilter` doesn't match any file (it's case-sensitive). Set `failIfNoProfiles=false` to allow empty runs. |
| Notebook activity: "notebook not found" | Imported notebook name doesn't match the `NotebookReference` (`bronze_ingest` etc.). |
| Notebook can't read config | `configRoot` doesn't point at the folder containing `defaults.yaml` + `profiles/`, or MI lacks read on the config container. |
| Ingest finds a profile that won't load | Discovery folder (`profilesFolderPath`) and `configRoot/profiles` are out of sync — point them at the same folder. |
| Publish fails authenticating to pool | MI not added as a pool user / missing GRANTs (see §2), or `sql_pool.temp_folder` not set. |
| Pool publish duplicates rows on retry | Enable `sql_pool.idempotent` (and `metrics_publish.idempotent`) + grant the MI DELETE. |
| Sessions queue / fail under load | `batchCount` exceeds Spark pool capacity — lower it or enable/expand autoscale. |
| Overlapping scheduled + manual runs | Expected to queue: pipeline `concurrency: 1` serializes whole-pipeline runs. |
