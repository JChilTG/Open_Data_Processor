/* ============================================================================
   Bronze Delta -> edw dedicated SQL pool : one-time setup
   ----------------------------------------------------------------------------
   Run this ONCE against the `edw` dedicated SQL pool (e.g. from a Synapse
   SQL script tab connected to the pool) BEFORE running the notebook.

   It is idempotent and safe to re-run.

   The workspace managed identity is assumed to already have all required
   permissions on this database, so this script only creates the schemas and
   operations tables - it does NOT create users or grant/set any permissions.
   ============================================================================ */

-------------------------------------------------------------------------------
-- 1. Schemas
--    `brz`        : holds the bronze data tables (one per discovered Delta table)
--    `operations` : holds control/watermark + run-log audit tables
--    (CREATE SCHEMA must be the only statement in its batch -> use dynamic SQL)
-------------------------------------------------------------------------------
IF SCHEMA_ID('brz') IS NULL
    EXEC('CREATE SCHEMA brz');
GO

IF SCHEMA_ID('operations') IS NULL
    EXEC('CREATE SCHEMA operations');
GO

-------------------------------------------------------------------------------
-- 2. Control / watermark table : one row per loaded table
-------------------------------------------------------------------------------
IF OBJECT_ID('operations.load_control') IS NULL
BEGIN
    CREATE TABLE operations.load_control
    (
        table_name           VARCHAR(256)   NOT NULL,
        source_path          VARCHAR(2048)  NULL,
        last_commit_version  BIGINT         NULL,
        last_loaded_utc      DATETIME2(3)   NULL,
        last_run_id          VARCHAR(64)    NULL,
        last_status          VARCHAR(32)    NULL
    )
    WITH ( DISTRIBUTION = ROUND_ROBIN, HEAP );
END
GO

-------------------------------------------------------------------------------
-- 3. Run-log audit table : append-only, one row per table per run
--    (plus a per-run summary row where table_name = '__RUN_SUMMARY__')
-------------------------------------------------------------------------------
IF OBJECT_ID('operations.load_log') IS NULL
BEGIN
    CREATE TABLE operations.load_log
    (
        run_id         VARCHAR(64)    NOT NULL,
        table_name     VARCHAR(256)   NOT NULL,
        source_path    VARCHAR(2048)  NULL,
        phase          VARCHAR(32)    NULL,   -- initial | incremental | skipped | summary
        start_version  BIGINT         NULL,
        end_version    BIGINT         NULL,
        rows_loaded    BIGINT         NULL,
        status         VARCHAR(32)    NULL,   -- SUCCESS | FAILED | SKIPPED
        error_message  VARCHAR(4000)  NULL,
        started_utc    DATETIME2(3)   NULL,
        ended_utc      DATETIME2(3)   NULL,
        duration_sec   DECIMAL(18,3)  NULL
    )
    WITH ( DISTRIBUTION = ROUND_ROBIN, HEAP );
END
GO

/* ============================================================================
   4. SOURCE-SIDE CHECKLIST (run in your Spark/Delta layer, NOT here)
   ----------------------------------------------------------------------------
   Change Data Feed must be enabled on every source Delta table for incremental
   loads. CDF only captures changes made AFTER it is enabled; the notebook's
   first run does a full snapshot, so enable CDF before/at first load.

   For each source table (PySpark):

       spark.sql('''
           ALTER TABLE delta.`abfss://<container>@<acct>.dfs.core.windows.net/<path>`
           SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
       ''')

   Or set the session default so all new Delta tables have it on:

       spark.conf.set(
           "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
           "true")

   (The config key name is historical; it is honoured by open-source Delta in
   the Synapse Spark runtime - no Databricks dependency is used.)
   ============================================================================ */
