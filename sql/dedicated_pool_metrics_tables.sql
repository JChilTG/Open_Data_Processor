-- Pre-create DDL for the dedicated SQL pool metrics tables.
--
-- Run this in the DEDICATED SQL pool (not Spark). notebooks/publish_metrics.py
-- appends to these via the Synapse Dedicated SQL Pool Connector (COPY + MSI
-- staging). Column names/order match the control Delta tables written by
-- bronze_ingest.py (control.bronze_run_log and control.bronze_column_profile).
--
-- These tables are small and queried broadly for trend/anomaly analysis, so
-- REPLICATE + CCI is a good default. Adjust the schema to match
-- metrics_publish.schema (default: operations).

CREATE SCHEMA operations;
GO

-- One row per notebook run (mirrors control.bronze_run_log).
CREATE TABLE operations.bronze_run_log
(
    [run_id]             VARCHAR(64),
    [profile_name]       VARCHAR(100),
    [batch_id]           VARCHAR(64),
    [files_discovered]   INT,
    [files_new]          INT,
    [files_processed]    INT,
    [files_failed]       INT,
    [rows_read]          BIGINT,
    [rows_loaded]        BIGINT,
    [rows_quarantined]   BIGINT,
    [quarantine_pct]     FLOAT(53),
    [status]             VARCHAR(30),
    [error_message]      NVARCHAR(4000),
    [started_at_utc]     DATETIME2(6),
    [ended_at_utc]       DATETIME2(6),
    [duration_seconds]   FLOAT(53),
    [processor_version]  VARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- One row per loaded column per run (mirrors control.bronze_column_profile).
CREATE TABLE operations.bronze_column_profile
(
    [profile_name]        VARCHAR(100),
    [table_name]          VARCHAR(128),
    [run_id]              VARCHAR(64),
    [batch_id]            VARCHAR(64),
    [column_name]         VARCHAR(128),
    [data_type]           VARCHAR(64),
    [row_count]           BIGINT,
    [null_count]          BIGINT,
    [null_pct]            FLOAT(53),
    [distinct_count]      BIGINT,
    [distinct_is_approx]  BIT,
    [min_value]           NVARCHAR(4000),
    [max_value]           NVARCHAR(4000),
    [profiled_at_utc]     DATETIME2(6),
    [processor_version]   VARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO
