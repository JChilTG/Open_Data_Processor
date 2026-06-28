-- Control / audit Delta tables for the bronze ingestion processor.
--
-- These are created automatically by notebooks/bronze_ingest.py on first run,
-- but the DDL is kept here for reference and for environments that prefer to
-- pre-create them. Run with Spark SQL (Synapse Spark notebook / Spark pool),
-- NOT in the dedicated SQL pool (these are Delta tables in the lakehouse).
--
-- Replace <control_base_path> with the value of control.base_path from
-- config/defaults.yaml, or remove the LOCATION clause to use the warehouse
-- default. The database name must match control.database.

CREATE DATABASE IF NOT EXISTS control;

-- One row per file processed. This table is the incremental watermark: a file
-- whose path already appears here with status = 'success' is skipped on the
-- next run.
CREATE TABLE IF NOT EXISTS control.bronze_ingestion_log (
    profile_name        STRING,
    source_file         STRING,
    file_size_bytes     BIGINT,
    file_modified_utc   TIMESTAMP,
    batch_id            STRING,
    run_id              STRING,
    rows_read           BIGINT,
    rows_loaded         BIGINT,
    rows_quarantined    BIGINT,
    quarantine_pct      DOUBLE,
    status              STRING,      -- success | failed | dead_letter | skipped
    attempt_count       INT,
    error_message       STRING,
    started_at_utc      TIMESTAMP,
    ended_at_utc        TIMESTAMP,
    processor_version   STRING
)
USING DELTA
-- LOCATION '<control_base_path>/bronze_ingestion_log'
;

-- One row per notebook run (a run processes a batch of files for one profile).
CREATE TABLE IF NOT EXISTS control.bronze_run_log (
    run_id              STRING,
    profile_name        STRING,
    batch_id            STRING,
    files_discovered    INT,
    files_new           INT,
    files_processed     INT,
    files_failed        INT,
    rows_read           BIGINT,
    rows_loaded         BIGINT,
    rows_quarantined    BIGINT,
    quarantine_pct      DOUBLE,
    status              STRING,      -- success | failed | skipped | circuit_breaker
    error_message       STRING,
    started_at_utc      TIMESTAMP,
    ended_at_utc        TIMESTAMP,
    duration_seconds    DOUBLE,
    processor_version   STRING
)
USING DELTA
-- LOCATION '<control_base_path>/bronze_run_log'
;

-- One row per loaded column per run: the per-load profiling metrics used for
-- trend lines and anomaly detection (null %, distinct counts, min/max).
CREATE TABLE IF NOT EXISTS control.bronze_column_profile (
    profile_name        STRING,
    table_name          STRING,
    run_id              STRING,
    batch_id            STRING,
    column_name         STRING,
    data_type           STRING,
    row_count           BIGINT,
    null_count          BIGINT,
    null_pct            DOUBLE,
    distinct_count      BIGINT,
    distinct_is_approx  BOOLEAN,     -- true = approx_count_distinct (HLL)
    min_value           STRING,      -- min/max stored as strings for a uniform column
    max_value           STRING,
    profiled_at_utc     TIMESTAMP,
    processor_version   STRING
)
USING DELTA
-- LOCATION '<control_base_path>/bronze_column_profile'
;
