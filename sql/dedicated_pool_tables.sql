-- Pre-create DDL for dedicated SQL pool bronze tables.
--
-- Run this in the DEDICATED SQL pool (not Spark). Pre-creating the table lets
-- you choose the distribution + index instead of taking the connector's
-- defaults (ROUND_ROBIN + CCI). The column list/order must match the projected
-- DataFrame written by notebooks/bronze_publish.py (after sql_pool.drop_columns
-- and sql_pool.rename are applied).
--
-- Spark/Delta -> dedicated SQL pool type mapping used here:
--   long           -> BIGINT
--   string         -> NVARCHAR(n)        (size to your data; pool NVARCHAR caps at 4000)
--   date           -> DATE
--   timestamp      -> DATETIME2
--   decimal(p,s)   -> DECIMAL(p,s)
--   sha2 hash      -> CHAR(64)

CREATE SCHEMA bronze;
GO

-- Example for the crm_customer profile (generate the up-to-date version per
-- feed with notebooks/generate_pool_ddl.py). Column names/order match the
-- bronze table; _bronze_run_id and _bronze_processor_version are dropped on
-- publish via sql_pool.drop_columns.
CREATE TABLE bronze.crm_customer
(
    [customer_id]               BIGINT,
    [email]                     NVARCHAR(256),
    [full_name]                 NVARCHAR(256),
    [signup_dt]                 DATE,
    [balance]                   DECIMAL(18, 2),
    [notes]                     NVARCHAR(4000),    -- free text; 4000 is the pool NVARCHAR max
    [_source_file]              NVARCHAR(1024),
    [_bronze_loaded_at_utc_ts]  DATETIME2(6),
    [_bronze_load_date]         DATE,
    [_bronze_profile_name]      VARCHAR(100),
    [_bronze_batch_id]          VARCHAR(64),
    [_bronze_change_key]        CHAR(64)           -- sha2-256 over business_fields
)
WITH
(
    -- HASH on the column dbt joins/filters on most to minimise data movement.
    -- Use ROUND_ROBIN instead if this is a pure landing/staging table.
    DISTRIBUTION = HASH([customer_id]),
    CLUSTERED COLUMNSTORE INDEX
);
GO
