#
# Incremental Delta (ADLS Gen2) -> Dedicated SQL Pool loader
# =============================================================================
# Run inside a Synapse notebook on a Spark 3.5 pool, in the same workspace as
# the target Dedicated SQL Pool.
#
# WHAT THIS DOES
#   Table sources are not hardcoded: each table is described by its own YAML
#   profile file in an ADLS Gen2 container named "metadata" (see
#   METADATA_CONTAINER_PATH below, and table_profiles/*.yaml alongside this
#   script for examples). For each profile: reads the Delta table from ADLS
#   Gen2, works out which rows are not yet present in the corresponding
#   Dedicated SQL Pool table (optionally narrowed first with a watermark
#   column for speed), stages only those candidate rows into the pool, then
#   performs a single set-based `INSERT ... SELECT ... WHERE NOT EXISTS` so
#   the pool itself - not Spark - does the authoritative de-dup. This is the
#   pattern that plays best with Dedicated SQL Pool's MPP engine (stage, then
#   push a set-based transform), rather than pulling target keys into Spark
#   for an anti-join.
#
# DESIGN NOTES / DEDICATED SQL POOL QUIRKS THIS WORKS AROUND
#   - The Spark connector for Dedicated SQL Pool (com.microsoft.spark.
#     sqlanalytics / `synapsesql`) has no UPSERT/MERGE mode - only
#     append/overwrite/ignore/errorifexists. Any "only load new rows" logic
#     has to be layered on top, which is what this script does.
#   - Bulk in/out of the pool from Spark always goes through the connector
#     (which stages via ADLS + COPY INTO/PolyBase under the hood). Plain JDBC
#     row-by-row writes are extremely slow on this engine and are avoided
#     entirely here - JDBC is only used for cheap scalar/metadata queries and
#     for the single set-based INSERT statement, never for bulk row transfer.
#   - The connector needs a staging storage linked service
#     (`spark.synapse.linkedService`) to move data in/out of the pool.
#   - Letting the connector auto-create a missing table (its `mode("overwrite")`
#     behavior) picks defaults you rarely want for a permanent table - e.g. it
#     maps every string column to NVARCHAR(4000), silently truncating anything
#     longer. So this script never relies on that: on first run it generates
#     its own `CREATE TABLE ... WITH (DISTRIBUTION = ..., ...)` from the Delta
#     schema (types mapped deliberately, distribution/structure configurable
#     per table, defaulting to ROUND_ROBIN + HEAP for small tables or
#     CLUSTERED COLUMNSTORE INDEX above ~5M rows per Microsoft's own sizing
#     guidance), then uses the connector purely as a bulk-load transport into
#     the table that already exists. The disposable staging table is created
#     the same explicit way each run too, by mirroring the target's *actual*
#     column types straight from INFORMATION_SCHEMA - so staging can never
#     drift from (or truncate relative to) the real target schema.
#   - Schema drift between the Delta source and the target table is handled,
#     not treated as an error: a column the source has gained is added to the
#     target with `ALTER TABLE ... ADD <col> <type> NULL` (always nullable,
#     never a DEFAULT - a DEFAULT would force Dedicated SQL Pool to rewrite
#     every existing row/distribution instead of a fast metadata-only change).
#     A column the source has lost is left alone on the target and simply
#     gets NULL inserted for new rows going forward, with a logged warning
#     (if that target column happens to be NOT NULL with no default, the
#     INSERT will fail with a clear constraint error, which is the right
#     place for that particular problem to surface).
#   - Dedicated SQL Pool has no reliable way to auto-increment/serialize
#     concurrent loads, and small SKUs (e.g. DW100c) have very few concurrent
#     query slots - tables are processed sequentially by default.
#   - NULL business keys break naive `t.key = s.key` joins (NULL <> NULL in
#     T-SQL), so the NOT EXISTS predicate is NULL-safe per key column.
#   - The NOT EXISTS check only guards against rows already in the *target*;
#     it says nothing about two rows sharing a key within the same source
#     read (a late correction, an upstream retry). Left alone that produces
#     a duplicate in the target, so every candidate batch is de-duplicated
#     on key_columns before staging (deduplicate_candidates - keeps the
#     highest watermark_column value per key when one is configured).
#   - Retries are not applied blindly: permission/object/syntax errors (SQL
#     error numbers in NON_RETRYABLE_SQL_ERROR_CODES) fail immediately rather
#     than burning ~70s of backoff on something that can't succeed on retry.
#     A one-time `SELECT 1` preflight (assert_pool_is_online) also catches a
#     paused pool once, up front, instead of discovering it per table.
#   - Two overlapping runs (e.g. a pipeline retry firing while a previous run
#     is still in flight) can't collide on the same staging table - each
#     run's staging table name is suffixed with a fresh RUN_ID. A stale
#     staging table left behind by a run that crashed before cleanup is
#     opportunistically dropped (age-gated via STALE_STAGING_TABLE_HOURS so a
#     genuinely concurrent run's own in-progress table is never touched).
#     This does NOT make two truly concurrent runs against the same target
#     safe from each other (Dedicated SQL Pool has no application-lock
#     mechanism) - enforce that at the orchestration layer if it's a risk.
#   - Complex Spark types (struct/array/map) are not supported by the pool at
#     all - the script fails fast with a clear message instead of letting the
#     connector throw a cryptic error mid-load.
#   - AAD auth reuses the notebook's own identity via mssparkutils - no
#     secrets/service principal are created or stored by this script. The
#     executing identity still needs SELECT/INSERT/CREATE TABLE/DROP TABLE on
#     the relevant schema in the pool (one-time grant), and read access to the
#     source ADLS Gen2 containers.
#   - No public internet access is required anywhere in this script. The
#     Spark pool talks to the Dedicated SQL Pool (JDBC, and the connector's
#     staging transport) over the workspace's own network - the same route
#     Synapse always uses between its own compute and its own SQL pool/
#     storage, private-endpoint-routed if the workspace has a managed VNet.
#     AAD token issuance via mssparkutils goes through Synapse's managed
#     outbound path too. Nothing here needs general outbound internet.
#
# PREREQUISITES (one-time, outside this script)
#   - A "metadata" ADLS Gen2 container (or a folder within one) holds one
#     YAML profile file per table - see METADATA_CONTAINER_PATH below and
#     table_profiles/*.yaml alongside this script for the exact fields.
#   - Target tables do NOT need to exist ahead of time - the script creates
#     any missing target table on its first run, from the Delta table's
#     schema (distribution/structure/type overrides come from that table's
#     YAML profile). Pre-create a table yourself only if you need something
#     the profile fields here don't cover.
#   - The identity running this notebook (interactive user, or pipeline
#     run-as identity) has SELECT/INSERT/CREATE TABLE/ALTER TABLE/DROP TABLE
#     rights on the target schema (and the staging schema, if different), and
#     read access to both the "metadata" container and the source ADLS Gen2
#     containers named in the profiles.
#   - A storage linked service exists in the workspace for the connector's
#     staging area, and that identity has Storage Blob Data Contributor on it.
#   - The Dedicated SQL Pool is resumed (not paused).

# ---- CELL: Parameters ----------------------------------------------------
# In Synapse Studio, mark this cell as a "Parameters" cell (toolbar toggle)
# if this notebook is invoked from a pipeline that needs to override values.

# Dedicated SQL Pool connection info (same workspace, so just the pool's own
# SQL endpoint + database/pool name).
SQL_POOL_SERVER = "<workspace-name>.sql.azuresynapse.net"
SQL_POOL_DATABASE = "<dedicated-pool-name>"

# Name of the ADLS Gen2 linked service the Dedicated SQL Pool connector should
# use as its staging area for COPY INTO/PolyBase moves. Usually the
# workspace's default storage linked service.
STAGING_STORAGE_LINKED_SERVICE = "<staging-storage-linked-service-name>"

# Schema to hold disposable staging tables. Defaults to each table's own
# target schema if left as None.
STAGING_SCHEMA = None
STAGING_TABLE_SUFFIX = "_stg_load"

# Set True to ignore watermark columns and re-evaluate every source row
# against the target with the NOT EXISTS check (useful for backfills /
# recovering from a gap). Normal daily runs should leave this False.
FULL_REFRESH = False

# Table sources are NOT hardcoded here - each table is described by its own
# YAML profile file in an ADLS Gen2 "metadata" container, loaded below (see
# the "Load table profiles" cell). This is the abfss:// folder holding those
# profiles, one *.yaml file per table.
METADATA_CONTAINER_PATH = "abfss://metadata@<storage-account>.dfs.core.windows.net/table-profiles/"

# Each profile file looks like (see table_profiles/orders.yaml and
# table_profiles/currencies.yaml alongside this script for full examples):
#
#   delta_path           : abfss:// path to the Delta table root in ADLS Gen2
#   target_schema        : schema of the target table in the Dedicated SQL Pool
#   target_table         : name of the target table in the Dedicated SQL Pool
#   key_columns          : business key column(s) used to detect "already
#                          loaded" rows. Required. The NOT EXISTS check is
#                          always NULL-safe, but a key that's actually NULL
#                          can't uniquely identify a row, so keep these NOT
#                          NULL in practice.
#   watermark_column      : optional monotonically increasing column (e.g. a
#                          modified/inserted timestamp or identity). When set,
#                          the source read is first pruned to rows greater
#                          than MAX(watermark_column) currently in the
#                          target, which is what keeps daily runs fast.
#                          Without it, the full Delta table is staged and
#                          de-duped every run.
#   distribution          : optional, "ROUND_ROBIN" (default) or "HASH". Only
#                          matters when the script has to CREATE the target
#                          table (i.e. first run). Ignored once the table
#                          already exists.
#   distribution_column   : required if distribution is HASH.
#   table_structure        : optional, "HEAP" or "CLUSTERED COLUMNSTORE INDEX".
#                          Defaults automatically from row count at first-run
#                          time (see SMALL_TABLE_ROW_THRESHOLD) if omitted.
#   column_type_overrides : optional mapping of {column_name: "SQL type"} for
#                          cases where the automatic Spark->SQL type mapping
#                          isn't what you want, e.g. {Notes: "NVARCHAR(MAX)"}.

# ---- CELL: Imports & setup -------------------------------------------------
import functools
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import yaml

from pyspark.sql import DataFrame, Window
from pyspark.sql.types import (
    ArrayType, MapType, StructType, StringType, BooleanType, ByteType,
    ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType,
    DateType, TimestampType, BinaryType,
)
from pyspark.sql.functions import col, lit, row_number, desc

from com.microsoft.spark.sqlanalytics.Constants import Constants

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("delta_to_dw")

spark.conf.set("spark.synapse.linkedService", STAGING_STORAGE_LINKED_SERVICE)

# Below this many source rows, a first-run CREATE TABLE defaults to HEAP
# rather than CLUSTERED COLUMNSTORE INDEX, per Microsoft's own guidance that
# columnstore only pays off at larger scale. Override per table via
# "table_structure" in TABLES if you want something different.
SMALL_TABLE_ROW_THRESHOLD = 5_000_000

# Unique per notebook execution. Used to suffix staging table names so two
# overlapping runs (e.g. a pipeline retry firing while a previous run is
# still in flight) can't collide on the same staging table. This does NOT by
# itself make two truly concurrent runs against the same target table safe -
# Dedicated SQL Pool has no sp_getapplock-style mechanism to lock against
# that. Enforce that at the orchestration layer (pipeline/trigger
# concurrency = 1) if overlapping runs are a real risk for you.
RUN_ID = uuid.uuid4().hex[:8]

# A staging table older than this is assumed to be an orphan from a run that
# crashed before its own cleanup ran (rather than a concurrently in-flight
# run's table), and gets dropped opportunistically at the start of the next
# run for that table.
STALE_STAGING_TABLE_HOURS = 6

JDBC_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
JDBC_URL = (
    f"jdbc:sqlserver://{SQL_POOL_SERVER}:1433;"
    f"databaseName={SQL_POOL_DATABASE};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.sql.azuresynapse.net;"
    "loginTimeout=30"
)
# AAD token audience for Azure SQL / Synapse SQL. mssparkutils is injected
# automatically into the Synapse notebook runtime - no import needed.
SQL_TOKEN_AUDIENCE = "https://database.windows.net/"


# ---- CELL: load table profiles (TABLES) ------------------------------------
# PyYAML ships with the default Synapse Spark 3.5 runtime; if your pool image
# doesn't have it, add it as a workspace/session-scoped library
# (`%pip install pyyaml` for a session-only install).
REQUIRED_PROFILE_FIELDS = ("delta_path", "target_schema", "target_table", "key_columns")


def _validate_profile(profile, source_path: str) -> None:
    if not isinstance(profile, dict):
        raise ValueError(f"Table profile {source_path} must be a YAML mapping, got {type(profile).__name__}")
    missing = [f for f in REQUIRED_PROFILE_FIELDS if not profile.get(f)]
    if missing:
        raise ValueError(f"Table profile {source_path} is missing required field(s): {missing}")
    if not isinstance(profile["key_columns"], list) or not profile["key_columns"]:
        raise ValueError(f"Table profile {source_path}: key_columns must be a non-empty list")
    if profile.get("distribution") == "HASH" and not profile.get("distribution_column"):
        raise ValueError(f"Table profile {source_path}: distribution_column is required when distribution is HASH")


def load_table_profiles(base_path: str):
    """Every *.yaml/*.yml file under base_path describes one table (see
    table_profiles/orders.yaml and table_profiles/currencies.yaml for
    examples). Reading table sources from files here - rather than hardcoding
    them into the notebook - means onboarding or changing a table is a file
    drop in ADLS, not a code change/redeploy of the notebook."""
    pattern = base_path.rstrip("/") + "/*.y*ml"
    files = spark.sparkContext.wholeTextFiles(pattern).collect()
    if not files:
        raise ValueError(f"No YAML table profiles found under {pattern}")

    profiles = []
    seen_targets = {}
    for file_path, content in sorted(files):
        try:
            profile = yaml.safe_load(content)
        except yaml.YAMLError as exc:
            raise ValueError(f"Failed to parse YAML table profile {file_path}: {exc}") from exc
        _validate_profile(profile, file_path)

        target_key = (profile["target_schema"].lower(), profile["target_table"].lower())
        if target_key in seen_targets:
            raise ValueError(
                f"Table profiles {seen_targets[target_key]} and {file_path} both target "
                f"{profile['target_schema']}.{profile['target_table']}"
            )
        seen_targets[target_key] = file_path
        profiles.append(profile)

    logger.info("Loaded %d table profile(s) from %s", len(profiles), base_path)
    return profiles


TABLES = load_table_profiles(METADATA_CONTAINER_PATH)


# ---- CELL: retry helper ----------------------------------------------------
# SQL Server/Synapse error numbers that mean "this will never succeed by
# retrying" - permission/object/syntax problems, not transient connectivity.
# Retrying these just delays the (inevitable, correct) failure by ~70s per
# call for no benefit.
NON_RETRYABLE_SQL_ERROR_CODES = {
    102,    # incorrect syntax near ...
    207,    # invalid column name
    208,    # invalid object name
    229,    # SELECT/INSERT/etc permission denied
    230,    # column permission denied
    18456,  # login failed
}


def _is_non_retryable(exc: Exception) -> bool:
    java_exc = getattr(exc, "java_exception", None)
    get_error_code = getattr(java_exc, "getErrorCode", None)
    if callable(get_error_code):
        try:
            return get_error_code() in NON_RETRYABLE_SQL_ERROR_CODES
        except Exception:
            return False
    return False


def with_retry(max_attempts=3, base_delay_seconds=10):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if _is_non_retryable(exc):
                        logger.error("%s hit a non-retryable SQL error, failing immediately: %s", func.__name__, exc)
                        raise
                    if attempt >= max_attempts:
                        logger.error("%s failed after %d attempts: %s", func.__name__, attempt, exc)
                        raise
                    delay = base_delay_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "%s attempt %d/%d failed (%s); retrying in %ds",
                        func.__name__, attempt, max_attempts, exc, delay,
                    )
                    time.sleep(delay)
        return wrapper
    return decorator


# ---- CELL: JDBC (metadata / DDL / the single set-based DML) ---------------
def quote_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def qualified(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


@with_retry()
def _new_jdbc_connection():
    jvm = spark._jvm
    jvm.Class.forName(JDBC_DRIVER_CLASS)
    props = jvm.java.util.Properties()
    token = mssparkutils.credentials.getToken(SQL_TOKEN_AUDIENCE)
    props.setProperty("accessToken", token)
    return jvm.java.sql.DriverManager.getConnection(JDBC_URL, props)


@with_retry()
def execute_sql(sql_text: str, timeout_seconds: int = 600) -> int:
    """Run DDL/DML with no expected result set. Returns affected row count."""
    conn = _new_jdbc_connection()
    try:
        stmt = conn.createStatement()
        try:
            stmt.setQueryTimeout(timeout_seconds)
            return stmt.executeUpdate(sql_text)
        finally:
            stmt.close()
    finally:
        conn.close()


@with_retry()
def query_scalar(sql_text: str, timeout_seconds: int = 60):
    """Run a SELECT expected to return a single row/column; returns that value or None."""
    conn = _new_jdbc_connection()
    try:
        stmt = conn.createStatement()
        try:
            stmt.setQueryTimeout(timeout_seconds)
            rs = stmt.executeQuery(sql_text)
            try:
                return rs.getObject(1) if rs.next() else None
            finally:
                rs.close()
        finally:
            stmt.close()
    finally:
        conn.close()


def assert_pool_is_online() -> None:
    """Cheap preflight check, meant to be called once before the per-table
    loop. Failing here once (in ~1 retry cycle) beats discovering the pool is
    paused only after burning retries on every JDBC call for every table."""
    try:
        query_scalar("SELECT 1")
    except Exception as exc:
        if "paused" in str(exc).lower():
            raise RuntimeError(
                f"Dedicated SQL Pool '{SQL_POOL_DATABASE}' on {SQL_POOL_SERVER} appears to be "
                f"paused. Resume it (Synapse Studio > Manage > SQL pools > Resume) and re-run."
            ) from exc
        raise


def _format_sql_type(data_type: str, char_len, num_precision, num_scale) -> str:
    """Rebuild a DDL type fragment (e.g. NVARCHAR(50), DECIMAL(18,2)) from
    INFORMATION_SCHEMA.COLUMNS parts. SQL Server/Synapse report -1 for MAX
    lengths."""
    t = data_type.lower()
    if t in ("nvarchar", "nchar", "varchar", "char", "varbinary", "binary"):
        length = "MAX" if char_len is not None and int(char_len) == -1 else str(int(char_len))
        return f"{data_type}({length})"
    if t in ("decimal", "numeric"):
        return f"{data_type}({int(num_precision)},{int(num_scale)})"
    if t in ("datetime2", "datetimeoffset", "time"):
        return f"{data_type}({int(num_scale)})" if num_scale is not None else data_type
    return data_type


@with_retry()
def query_target_column_defs(schema: str, table: str):
    """Ordered [(column_name, ddl_type), ...] straight from the pool's own
    catalog, or [] if the table doesn't exist. Used both to check existence
    and to build a staging table that exactly matches the real target types
    (never inferred from Spark, so it can never truncate relative to it)."""
    conn = _new_jdbc_connection()
    try:
        stmt = conn.createStatement()
        try:
            stmt.setQueryTimeout(60)
            sql_text = (
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
                "NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
                "ORDER BY ORDINAL_POSITION"
            )
            rs = stmt.executeQuery(sql_text)
            try:
                defs = []
                while rs.next():
                    defs.append((
                        rs.getString(1),
                        _format_sql_type(rs.getString(2), rs.getObject(3), rs.getObject(4), rs.getObject(5)),
                    ))
                return defs
            finally:
                rs.close()
        finally:
            stmt.close()
    finally:
        conn.close()


@with_retry()
def find_stale_staging_tables(schema: str, name_like_pattern: str, older_than_hours: int):
    """Staging tables matching name_like_pattern (a T-SQL LIKE pattern) whose
    create_date is older than older_than_hours - i.e. left behind by a run
    that crashed before its own finally-block cleanup ran. Age-gated so this
    can't mistake a concurrently in-flight run's (recently created) staging
    table for an orphan."""
    conn = _new_jdbc_connection()
    try:
        stmt = conn.createStatement()
        try:
            stmt.setQueryTimeout(30)
            sql_text = (
                "SELECT t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
                f"WHERE s.name = '{schema}' AND t.name LIKE '{name_like_pattern}' "
                f"AND t.create_date < DATEADD(HOUR, -{older_than_hours}, SYSUTCDATETIME())"
            )
            rs = stmt.executeQuery(sql_text)
            try:
                names = []
                while rs.next():
                    names.append(rs.getString(1))
                return names
            finally:
                rs.close()
        finally:
            stmt.close()
    finally:
        conn.close()


# ---- CELL: schema validation / alignment -----------------------------------
UNSUPPORTED_TYPES = (ArrayType, MapType, StructType)


def assert_supported_schema(df: DataFrame, table_label: str) -> None:
    bad = [f.name for f in df.schema.fields if isinstance(f.dataType, UNSUPPORTED_TYPES)]
    if bad:
        raise ValueError(
            f"[{table_label}] Dedicated SQL Pool does not support complex column "
            f"types (struct/array/map). Flatten or drop these columns before "
            f"loading: {bad}"
        )


def align_to_target_columns(df: DataFrame, target_column_defs, table_label: str) -> DataFrame:
    """Select+reorder df columns to exactly match the target table's current
    column list (case-insensitive match, since the pool's default collation
    is case-insensitive).

    Columns the source no longer produces (a field removed upstream) are NOT
    a hard failure: they're filled with NULL so the load keeps working, on
    the assumption the target column is nullable (if it genuinely isn't, the
    INSERT will fail with a clear constraint-violation error from the pool,
    which is the right place for that to surface). Source columns not
    (yet/anymore) in the target are dropped with a warning - by the time this
    runs, any new fields have already been added to the target via
    add_missing_columns, so this only fires if that step was skipped."""
    source_lookup = {c.lower(): c for c in df.columns}
    type_by_name = dict(target_column_defs)
    target_names = list(type_by_name.keys())

    missing_in_source = [n for n in target_names if n.lower() not in source_lookup]
    if missing_in_source:
        logger.warning(
            "[%s] Target column(s) no longer produced by the source - inserting "
            "NULL for these going forward: %s", table_label, missing_in_source,
        )

    extra_in_source = [c for c in df.columns if c.lower() not in {n.lower() for n in target_names}]
    if extra_in_source:
        logger.warning("[%s] Ignoring source columns not present in target: %s", table_label, extra_in_source)

    projected = []
    for name in target_names:
        if name.lower() in source_lookup:
            projected.append(col(source_lookup[name.lower()]).alias(name))
        else:
            projected.append(lit(None).cast(sql_type_to_spark_type(type_by_name[name])).alias(name))
    return df.select(*projected)


def deduplicate_candidates(df: DataFrame, key_columns, watermark_column, table_label: str) -> DataFrame:
    """The NOT EXISTS check only guards against rows already in the target -
    it says nothing about two rows sharing a key within the same source read
    (a late correction landing next to the original row, an upstream retry,
    etc.). Left unhandled, both would pass NOT EXISTS and both get inserted,
    producing a duplicate in the target. So the candidate batch is always
    de-duplicated on key_columns before staging: keeping the highest
    watermark_column value per key when one is configured, otherwise an
    arbitrary-but-single row per key."""
    logger.info("[%s] De-duplicating candidate rows on key columns: %s", table_label, key_columns)
    if watermark_column:
        window = Window.partitionBy(*[col(c) for c in key_columns]).orderBy(desc(col(watermark_column)))
        return (
            df.withColumn("_dedupe_rn", row_number().over(window))
            .where(col("_dedupe_rn") == 1)
            .drop("_dedupe_rn")
        )
    return df.dropDuplicates(key_columns)


def build_null_safe_predicate(key_columns, target_alias="t", staging_alias="s") -> str:
    parts = []
    for c in key_columns:
        ident = quote_ident(c)
        parts.append(
            f"(({target_alias}.{ident} = {staging_alias}.{ident}) "
            f"OR ({target_alias}.{ident} IS NULL AND {staging_alias}.{ident} IS NULL))"
        )
    return " AND ".join(parts)


# ---- CELL: type mapping + DDL generation (create table / add column) ------
def spark_type_to_sql_type(spark_type, override: Optional[str] = None) -> str:
    """Deliberate Spark -> Dedicated SQL Pool type mapping, used instead of
    the connector's own auto-create inference (which defaults every string
    column to NVARCHAR(4000) and silently truncates anything longer)."""
    if override:
        return override
    if isinstance(spark_type, StringType):
        return "NVARCHAR(4000)"
    if isinstance(spark_type, BooleanType):
        return "BIT"
    if isinstance(spark_type, (ByteType, ShortType)):
        # SQL TINYINT is unsigned 0-255; Spark's ByteType is signed -128..127,
        # so TINYINT would silently overflow on any negative byte value.
        return "SMALLINT"
    if isinstance(spark_type, IntegerType):
        return "INT"
    if isinstance(spark_type, LongType):
        return "BIGINT"
    if isinstance(spark_type, FloatType):
        return "REAL"
    if isinstance(spark_type, DoubleType):
        return "FLOAT"
    if isinstance(spark_type, DecimalType):
        precision = min(spark_type.precision, 38)
        scale = min(spark_type.scale, precision)
        return f"DECIMAL({precision},{scale})"
    if isinstance(spark_type, DateType):
        return "DATE"
    if isinstance(spark_type, TimestampType):
        # (6) matches Spark's microsecond timestamp resolution exactly.
        return "DATETIME2(6)"
    if isinstance(spark_type, BinaryType):
        return "VARBINARY(MAX)"
    raise ValueError(
        f"No default SQL Pool type mapping for Spark type {spark_type}; "
        f"add a column_type_overrides entry for this column."
    )


def sql_type_to_spark_type(sql_type: str):
    """Reverse of spark_type_to_sql_type, best-effort - only used to type a
    NULL placeholder for a target column the source no longer produces."""
    t = sql_type.lower()
    base = t.split("(")[0]
    if base == "bigint":
        return LongType()
    if base == "int":
        return IntegerType()
    if base in ("smallint", "tinyint"):
        return ShortType()
    if base == "bit":
        return BooleanType()
    if base in ("decimal", "numeric"):
        try:
            p, s = t[t.index("(") + 1:t.index(")")].split(",")
            return DecimalType(int(p), int(s))
        except Exception:
            return DecimalType(38, 18)
    if base == "float":
        return DoubleType()
    if base == "real":
        return FloatType()
    if base == "date":
        return DateType()
    if base in ("datetime2", "datetimeoffset", "datetime", "smalldatetime"):
        return TimestampType()
    if base in ("varbinary", "binary"):
        return BinaryType()
    return StringType()


def build_create_table_sql(df, schema, table, key_columns, distribution, distribution_column,
                            table_structure, column_type_overrides) -> str:
    overrides = column_type_overrides or {}
    col_defs = []
    for f in df.schema.fields:
        sql_type = spark_type_to_sql_type(f.dataType, overrides.get(f.name))
        nullability = "NOT NULL" if (f.name in key_columns or not f.nullable) else "NULL"
        col_defs.append(f"{quote_ident(f.name)} {sql_type} {nullability}")
    col_defs_sql = ",\n    ".join(col_defs)

    if distribution == "HASH":
        if not distribution_column:
            raise ValueError("distribution_column is required when distribution='HASH'")
        dist_type = next(
            (spark_type_to_sql_type(f.dataType, overrides.get(f.name))
             for f in df.schema.fields if f.name == distribution_column), None,
        )
        if dist_type and "MAX" in dist_type:
            raise ValueError(
                f"distribution_column '{distribution_column}' maps to {dist_type}, which "
                f"cannot be a HASH distribution column in Dedicated SQL Pool (no MAX types)."
            )
        dist_clause = f"HASH({quote_ident(distribution_column)})"
    else:
        dist_clause = "ROUND_ROBIN"

    return (
        f"CREATE TABLE {qualified(schema, table)} (\n    {col_defs_sql}\n)\n"
        f"WITH (\n    DISTRIBUTION = {dist_clause},\n    {table_structure}\n)"
    )


def build_staging_table_sql(schema, table, column_defs) -> str:
    """Staging table types are copied verbatim from the real target's
    catalog (not re-inferred from Spark), so staging can never drift from or
    truncate relative to the actual target column definitions."""
    col_defs_sql = ",\n    ".join(f"{quote_ident(name)} {sql_type} NULL" for name, sql_type in column_defs)
    return (
        f"CREATE TABLE {qualified(schema, table)} (\n    {col_defs_sql}\n)\n"
        f"WITH (\n    DISTRIBUTION = ROUND_ROBIN,\n    HEAP\n)"
    )


def add_missing_columns(schema, table, new_fields, column_type_overrides, table_label):
    """Add columns the source has gained since the target table was created.
    Always added as NULL with no DEFAULT: on Dedicated SQL Pool that keeps
    the ALTER a fast metadata-only operation, whereas a DEFAULT would force a
    full rewrite of every existing row/distribution. Returns the list of
    (name, sql_type) tuples added, to fold into the caller's column list."""
    overrides = column_type_overrides or {}
    added = []
    for f in new_fields:
        sql_type = spark_type_to_sql_type(f.dataType, overrides.get(f.name))
        logger.info("[%s] Source has a new column - adding it to the target: %s %s",
                    table_label, f.name, sql_type)
        execute_sql(f"ALTER TABLE {qualified(schema, table)} ADD {quote_ident(f.name)} {sql_type} NULL")
        added.append((f.name, sql_type))
    return added


# ---- CELL: bulk data movement in/out of the pool (connector only) ---------
@with_retry()
def write_dataframe_to_pool(df: DataFrame, schema: str, table: str, mode: str) -> None:
    three_part_name = f"{SQL_POOL_DATABASE}.{schema}.{table}"
    (
        df.write.option(Constants.SERVER, SQL_POOL_SERVER)
        .mode(mode)
        .synapsesql(three_part_name)
    )


# ---- CELL: per-table sync --------------------------------------------------
@dataclass
class SyncResult:
    table_label: str
    status: str
    rows_candidate: int = 0
    rows_inserted: int = 0
    detail: str = ""


def sync_table(conf: dict) -> SyncResult:
    target_schema = conf["target_schema"]
    target_table = conf["target_table"]
    key_columns = conf["key_columns"]
    watermark_column = conf.get("watermark_column")
    delta_path = conf["delta_path"]
    column_type_overrides = conf.get("column_type_overrides")
    label = f"{target_schema}.{target_table}"

    staging_schema = STAGING_SCHEMA or target_schema
    staging_table_prefix = f"{target_table}{STAGING_TABLE_SUFFIX}"
    staging_table = f"{staging_table_prefix}_{RUN_ID}"

    if not key_columns:
        raise ValueError(f"[{label}] key_columns is required")

    df = spark.read.format("delta").load(delta_path)
    assert_supported_schema(df, label)

    target_column_defs = query_target_column_defs(target_schema, target_table)

    # --- Bootstrap: target table doesn't exist yet -> create + one-shot load
    if not target_column_defs:
        row_count = df.count()
        structure = conf.get("table_structure") or (
            "HEAP" if row_count < SMALL_TABLE_ROW_THRESHOLD else "CLUSTERED COLUMNSTORE INDEX"
        )
        create_ddl = build_create_table_sql(
            df, target_schema, target_table, key_columns,
            conf.get("distribution", "ROUND_ROBIN"), conf.get("distribution_column"),
            structure, column_type_overrides,
        )
        logger.info("[%s] Target table not found - creating it:\n%s", label, create_ddl)
        execute_sql(create_ddl)
        if row_count == 0:
            return SyncResult(label, "CREATED_EMPTY", detail="target table created; source has no rows yet")
        write_dataframe_to_pool(df, target_schema, target_table, mode="append")
        logger.info("[%s] Bootstrap load complete: %d rows.", label, row_count)
        return SyncResult(label, "BOOTSTRAPPED", rows_candidate=row_count, rows_inserted=row_count)

    # --- Schema evolution: add any columns the source has gained ----------
    target_names_lower = {name.lower() for name, _ in target_column_defs}
    new_fields = [f for f in df.schema.fields if f.name.lower() not in target_names_lower]
    if new_fields:
        target_column_defs = target_column_defs + add_missing_columns(
            target_schema, target_table, new_fields, column_type_overrides, label
        )

    # Columns the source has lost since target creation are handled inside
    # align_to_target_columns (NULL-filled, not a hard failure).
    df = align_to_target_columns(df, target_column_defs, label)
    target_columns = [name for name, _ in target_column_defs]

    # --- Narrow the source read with the watermark, when available --------
    if watermark_column and not FULL_REFRESH:
        max_watermark = query_scalar(
            f"SELECT MAX({quote_ident(watermark_column)}) FROM {qualified(target_schema, target_table)}"
        )
        if max_watermark is not None:
            df = df.where(col(watermark_column) > max_watermark)
            logger.info("[%s] Watermark filter: %s > %s", label, watermark_column, max_watermark)

    candidate_count = df.limit(1).count()
    if candidate_count == 0:
        return SyncResult(label, "SKIPPED", detail="no candidate rows after watermark filter")

    df = deduplicate_candidates(df, key_columns, watermark_column, label)
    candidate_count = df.count()
    logger.info("[%s] %d candidate row(s) to stage (post de-dup).", label, candidate_count)

    try:
        for stale_name in find_stale_staging_tables(staging_schema, f"{staging_table_prefix}_%", STALE_STAGING_TABLE_HOURS):
            try:
                execute_sql(f"DROP TABLE {qualified(staging_schema, stale_name)}")
                logger.info("[%s] Dropped orphaned staging table from a previous run: %s", label, stale_name)
            except Exception as stale_exc:
                logger.warning("[%s] Could not drop orphaned staging table %s: %s", label, stale_name, stale_exc)

        execute_sql(build_staging_table_sql(staging_schema, staging_table, target_column_defs))
        write_dataframe_to_pool(df, staging_schema, staging_table, mode="append")

        col_list = ", ".join(quote_ident(c) for c in target_columns)
        predicate = build_null_safe_predicate(key_columns)
        insert_sql = (
            f"INSERT INTO {qualified(target_schema, target_table)} ({col_list})\n"
            f"SELECT {col_list} FROM {qualified(staging_schema, staging_table)} AS s\n"
            f"WHERE NOT EXISTS (\n"
            f"    SELECT 1 FROM {qualified(target_schema, target_table)} AS t\n"
            f"    WHERE {predicate}\n"
            f")"
        )
        inserted = execute_sql(insert_sql, timeout_seconds=1800)
        logger.info("[%s] Inserted %d new row(s) (%d candidates, %d already present).",
                    label, inserted, candidate_count, candidate_count - inserted)
        return SyncResult(label, "OK", rows_candidate=candidate_count, rows_inserted=inserted)
    finally:
        try:
            execute_sql(
                f"IF OBJECT_ID('{staging_schema}.{staging_table}', 'U') IS NOT NULL "
                f"DROP TABLE {qualified(staging_schema, staging_table)}"
            )
        except Exception as cleanup_exc:
            logger.warning("[%s] Failed to drop staging table %s.%s: %s",
                           label, staging_schema, staging_table, cleanup_exc)


# ---- CELL: main driver ------------------------------------------------------
def main():
    assert_pool_is_online()

    results = []
    for conf in TABLES:
        label = f"{conf['target_schema']}.{conf['target_table']}"
        try:
            results.append(sync_table(conf))
        except Exception as exc:
            logger.exception("[%s] Sync failed", label)
            results.append(SyncResult(label, "FAILED", detail=str(exc)))

    logger.info("=" * 80)
    logger.info("%-30s %-12s %10s %10s  %s", "TABLE", "STATUS", "CANDIDATE", "INSERTED", "DETAIL")
    for r in results:
        logger.info("%-30s %-12s %10d %10d  %s", r.table_label, r.status, r.rows_candidate, r.rows_inserted, r.detail)

    failed = [r for r in results if r.status == "FAILED"]
    if failed:
        raise RuntimeError(f"{len(failed)} table(s) failed to sync: {[r.table_label for r in failed]}")


main()
