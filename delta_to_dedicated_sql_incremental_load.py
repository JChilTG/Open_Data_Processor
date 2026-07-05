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
#   Gen2, filters it to rows whose watermark_column is greater than
#   MAX(watermark_column) already in the target - no business key or
#   NOT EXISTS/anti-join check involved, "already loaded" is decided purely
#   by the watermark cursor - stages those rows, then commits them with a
#   single set-based `INSERT ... SELECT` (staging is still used even though
#   there's no de-dup predicate to push down: it's what makes the commit into
#   the pool retry-safe - see load_target_table_fresh/load_staging_table).
#   watermark_column defaults to "_bronze_loaded_at_utc_ts" (a bronze-layer
#   ingestion timestamp, stamped once per load batch) if a profile doesn't
#   specify one.
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
#   - HEAP/CLUSTERED INDEX tables (unlike CLUSTERED COLUMNSTORE INDEX) are
#     subject to Dedicated SQL Pool's classic 8,060-byte max row size. Since
#     every string column defaults to NVARCHAR(4000) (8,000 bytes) to avoid
#     silent truncation, a "small" table with just two or three such columns
#     can blow past that on its own. estimate_heap_row_bytes sizes this up
#     before deciding HEAP vs. columnstore, and forces columnstore (which
#     isn't subject to the limit) even under SMALL_TABLE_ROW_THRESHOLD when
#     HEAP would exceed it - an explicit table_structure: HEAP is still
#     honored, but with a logged warning if it looks like it'll fail.
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
#   - watermark_column is load-bearing, not a performance nicety: it's the
#     only signal used to decide "already loaded". It's validated as present
#     in the source before anything else runs. If the source ever stops
#     producing it, the load fails loudly instead of silently going quiet
#     (which is what would happen if it were merely NULL-filled like any
#     other dropped column - see align_to_target_columns).
#   - Because there's no per-row identity check, this scheme assumes the
#     source's watermark_column is assigned once, atomically, per ingestion
#     batch, and never changes afterward. If a batch's rows only partially
#     land in the target (a failure mid-write) before this script's own
#     retry-safe staging path was in place, rows sharing that exact
#     watermark value would not be revisited on the next run, since the
#     filter is a strict `>` against the batch's own watermark value. Keep
#     batches atomic on the source side (all-or-nothing per
#     watermark_column value) for this to hold.
#   - FULL_REFRESH now TRUNCATEs the target before reloading everything: with
#     no business key to de-duplicate against, blindly re-staging the whole
#     source on top of an already-loaded target would just double every row.
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

# Set True to TRUNCATE each target table and reload its entire source,
# ignoring the watermark cursor (for backfills / recovering from a gap).
# There is no business-key de-dup to fall back on here, so this always wipes
# the target first rather than re-staging on top of what's already there.
# Normal daily runs should leave this False.
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
#   watermark_column      : monotonically increasing column, assigned once per
#                          ingestion batch (e.g. a bronze load timestamp).
#                          This is the *sole* mechanism used to decide
#                          "already loaded" - there is no business-key check.
#                          The source read is pruned to rows greater than
#                          MAX(watermark_column) currently in the target.
#                          Optional in the profile: defaults to
#                          DEFAULT_WATERMARK_COLUMN ("_bronze_loaded_at_utc_ts")
#                          if omitted. Must exist in the source Delta table.
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
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import yaml

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType, MapType, StructType, StringType, BooleanType, ByteType,
    ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType,
    DateType, TimestampType, BinaryType,
)
from pyspark.sql.functions import col, lit

from com.microsoft.spark.sqlanalytics.Constants import Constants

# Synapse's Livy/Spark driver process typically attaches its own handlers to
# the root logger before this cell runs, which makes a plain basicConfig()
# call a silent no-op (by design: it does nothing if the root logger already
# has handlers). force=True tears those out and installs ours instead, and
# stream=sys.stdout makes sure it lands in the notebook's visible cell
# output rather than stderr, which some notebook front-ends don't surface.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger("delta_to_dw")

spark.conf.set("spark.synapse.linkedService", STAGING_STORAGE_LINKED_SERVICE)

# Below this many source rows, a first-run CREATE TABLE defaults to HEAP
# rather than CLUSTERED COLUMNSTORE INDEX, per Microsoft's own guidance that
# columnstore only pays off at larger scale. Override via "table_structure"
# in that table's YAML profile if you want something different.
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
# "DW" is mssparkutils' documented audience shorthand for the Dedicated SQL
# Pool / Synapse SQL resource. mssparkutils is injected automatically into
# the Synapse notebook runtime - no import needed.
SQL_TOKEN_AUDIENCE = "DW"


# ---- CELL: load table profiles (TABLES) ------------------------------------
# PyYAML ships with the default Synapse Spark 3.5 runtime; if your pool image
# doesn't have it, add it as a workspace/session-scoped library
# (`%pip install pyyaml` for a session-only install).
REQUIRED_PROFILE_FIELDS = ("delta_path", "target_schema", "target_table")

# Used when a profile doesn't specify watermark_column - a bronze-layer
# ingestion timestamp, stamped once per load batch, that this script uses as
# the sole "already loaded" cursor (see sync_table).
DEFAULT_WATERMARK_COLUMN = "_bronze_loaded_at_utc_ts"


def _validate_profile(profile, source_path: str) -> None:
    if not isinstance(profile, dict):
        raise ValueError(f"Table profile {source_path} must be a YAML mapping, got {type(profile).__name__}")
    missing = [f for f in REQUIRED_PROFILE_FIELDS if not profile.get(f)]
    if missing:
        raise ValueError(f"Table profile {source_path} is missing required field(s): {missing}")
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
    """Run a SELECT expected to return a single row/column; returns that value
    (as a string) or None. Deliberately uses getString(), not getObject():
    Py4J auto-converts Java String/Integer/Long/Boolean/Double across the
    gateway, but NOT java.sql.Timestamp/Date or java.math.BigDecimal - those
    come back as opaque Java object proxies that PySpark can't use as a
    literal in a Column expression (e.g. col(x) > max_watermark would break
    the moment a watermark column is a date/time/decimal type, which is the
    common case). Getting a string and letting the caller cast it back to
    the right Spark type sidesteps that entirely."""
    conn = _new_jdbc_connection()
    try:
        stmt = conn.createStatement()
        try:
            stmt.setQueryTimeout(timeout_seconds)
            rs = stmt.executeQuery(sql_text)
            try:
                return rs.getString(1) if rs.next() else None
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


# Dedicated SQL Pool inherits SQL Server's classic per-row byte limit for
# rowstore structures (HEAP and CLUSTERED INDEX) - it does NOT apply to
# CLUSTERED COLUMNSTORE INDEX. Defaulting every string column to NVARCHAR(4000)
# (8000 bytes) means just two or three such columns on a HEAP table can blow
# past this on their own, and CREATE TABLE/ALTER TABLE ADD fail outright.
HEAP_MAX_ROW_BYTES = 8060
# Deliberately conservative allowance for row header, null bitmap, and the
# variable-length column offset array - the real overhead is smaller, but
# erring high only ever pushes a borderline table to columnstore instead of
# HEAP, never the other way around.
HEAP_ROW_OVERHEAD_BYTES = 100


def _sql_type_byte_width(sql_type: str) -> int:
    """Estimated in-row byte footprint of a SQL Pool column type, for sizing
    against HEAP_MAX_ROW_BYTES. MAX types are stored off-row (a small
    pointer in the row), unlike their fixed-length counterparts."""
    t = sql_type.lower()
    base = t.split("(")[0]

    def _len_arg():
        try:
            inner = t[t.index("(") + 1:t.index(")")]
            return None if inner == "max" else int(inner)
        except ValueError:
            return None

    if base in ("nvarchar", "nchar"):
        n = _len_arg()
        return 24 if n is None else 2 * n
    if base in ("varchar", "char", "varbinary", "binary"):
        n = _len_arg()
        return 24 if n is None else n
    if base == "bigint":
        return 8
    if base == "int":
        return 4
    if base == "smallint":
        return 2
    if base in ("tinyint", "bit"):
        return 1
    if base == "real":
        return 4
    if base == "float":
        return 8
    if base in ("decimal", "numeric"):
        try:
            precision = int(t[t.index("(") + 1:t.index(",")])
        except ValueError:
            precision = 38
        if precision <= 9:
            return 5
        if precision <= 19:
            return 9
        if precision <= 28:
            return 13
        return 17
    if base == "date":
        return 3
    if base in ("datetime2", "datetimeoffset", "datetime", "smalldatetime"):
        return 8
    return 8000  # unrecognized (e.g. an unusual column_type_overrides value) - assume worst case


def estimate_heap_row_bytes(df, column_type_overrides) -> int:
    overrides = column_type_overrides or {}
    total = HEAP_ROW_OVERHEAD_BYTES
    for f in df.schema.fields:
        sql_type = spark_type_to_sql_type(f.dataType, overrides.get(f.name))
        total += _sql_type_byte_width(sql_type)
    return total


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


def build_create_table_sql(df, schema, table, distribution, distribution_column,
                            table_structure, column_type_overrides) -> str:
    # Every column is created NULLable, even columns whose Spark schema
    # claims nullable=False - Delta/Parquet nullability metadata is not a
    # trustworthy guarantee about the actual data, and forcing a stricter
    # NOT NULL than the source can really promise means a load fails
    # outright the day a real null shows up.
    overrides = column_type_overrides or {}
    col_defs = []
    for f in df.schema.fields:
        sql_type = spark_type_to_sql_type(f.dataType, overrides.get(f.name))
        col_defs.append(f"{quote_ident(f.name)} {sql_type} NULL")
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
    (name, sql_type) tuples added, to fold into the caller's column list.

    Guarded with IF NOT EXISTS: execute_sql retries on transient failure, and
    if a prior attempt's ALTER actually committed server-side but the client
    never saw the acknowledgment (e.g. a dropped connection), an unguarded
    retry would hit "column already exists" instead of quietly no-op'ing."""
    overrides = column_type_overrides or {}
    added = []
    for f in new_fields:
        sql_type = spark_type_to_sql_type(f.dataType, overrides.get(f.name))
        logger.info("[%s] Source has a new column - adding it to the target: %s %s",
                    table_label, f.name, sql_type)
        execute_sql(
            f"IF NOT EXISTS (\n"
            f"    SELECT 1 FROM sys.columns c JOIN sys.tables t ON c.object_id = t.object_id\n"
            f"    JOIN sys.schemas s ON t.schema_id = s.schema_id\n"
            f"    WHERE s.name = '{schema}' AND t.name = '{table}' AND c.name = '{f.name}'\n"
            f")\n"
            f"BEGIN\n"
            f"    ALTER TABLE {qualified(schema, table)} ADD {quote_ident(f.name)} {sql_type} NULL\n"
            f"END"
        )
        added.append((f.name, sql_type))
    return added


# ---- CELL: bulk data movement in/out of the pool (connector only) ---------
def _write_dataframe(df: DataFrame, schema: str, table: str, mode: str) -> None:
    three_part_name = f"{SQL_POOL_DATABASE}.{schema}.{table}"
    (
        df.write.option(Constants.SERVER, SQL_POOL_SERVER)
        .mode(mode)
        .synapsesql(three_part_name)
    )


@with_retry()
def load_target_table_fresh(df: DataFrame, schema: str, table: str) -> None:
    """First load into a table this run just created. TRUNCATE (idempotent,
    metadata-only) then append as a single retryable unit: if the append
    fails partway through and this retries, it must start from a guaranteed-
    empty table - otherwise the retry would double up whatever the failed
    attempt already managed to write. There's no business-key check anywhere
    in this script to catch that after the fact, so this has to hold on its
    own."""
    execute_sql(f"TRUNCATE TABLE {qualified(schema, table)}")
    _write_dataframe(df, schema, table, mode="append")


@with_retry()
def load_staging_table(df: DataFrame, schema: str, table: str, column_defs) -> None:
    """Same reasoning as load_target_table_fresh, for the disposable staging
    table: drop-if-exists + create + append as one retryable unit, so a
    retry after a partial write failure can't double-insert into staging -
    which would otherwise commit straight into the target as real duplicate
    rows, since the final INSERT has no de-dup predicate of its own."""
    execute_sql(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL DROP TABLE {qualified(schema, table)}")
    execute_sql(build_staging_table_sql(schema, table, column_defs))
    _write_dataframe(df, schema, table, mode="append")


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
    watermark_column = conf.get("watermark_column") or DEFAULT_WATERMARK_COLUMN
    delta_path = conf["delta_path"]
    column_type_overrides = conf.get("column_type_overrides")
    label = f"{target_schema}.{target_table}"

    staging_schema = STAGING_SCHEMA or target_schema
    staging_table_prefix = f"{target_table}{STAGING_TABLE_SUFFIX}"
    staging_table = f"{staging_table_prefix}_{RUN_ID}"

    df = spark.read.format("delta").load(delta_path)
    assert_supported_schema(df, label)

    # watermark_column is the only mechanism left for deciding "already
    # loaded" - unlike an ordinary column, it can't be allowed to quietly
    # NULL-fill if the source stops producing it (see align_to_target_columns
    # for how that's handled for every other column), so it's checked here,
    # before anything else, with a hard failure.
    if watermark_column.lower() not in {c.lower() for c in df.columns}:
        raise ValueError(
            f"[{label}] watermark_column '{watermark_column}' was not found in the source Delta "
            f"table - it's required (there is no business-key fallback for detecting new rows)."
        )

    target_column_defs = query_target_column_defs(target_schema, target_table)

    # --- Bootstrap: target table doesn't exist yet -> create + one-shot load
    if not target_column_defs:
        row_count = df.count()
        estimated_row_bytes = estimate_heap_row_bytes(df, column_type_overrides)
        explicit_structure = conf.get("table_structure")
        if explicit_structure:
            structure = explicit_structure
            if structure == "HEAP" and estimated_row_bytes > HEAP_MAX_ROW_BYTES:
                logger.warning(
                    "[%s] table_structure is explicitly HEAP but the estimated row width "
                    "(~%d bytes) exceeds Dedicated SQL Pool's %d-byte HEAP row limit - "
                    "CREATE TABLE will likely fail. Consider CLUSTERED COLUMNSTORE INDEX or "
                    "narrowing column_type_overrides.",
                    label, estimated_row_bytes, HEAP_MAX_ROW_BYTES,
                )
        elif row_count < SMALL_TABLE_ROW_THRESHOLD and estimated_row_bytes <= HEAP_MAX_ROW_BYTES:
            structure = "HEAP"
        else:
            structure = "CLUSTERED COLUMNSTORE INDEX"
            if row_count < SMALL_TABLE_ROW_THRESHOLD:
                logger.info(
                    "[%s] Using CLUSTERED COLUMNSTORE INDEX despite row count (%d) being under "
                    "the small-table threshold - estimated row width (~%d bytes) would exceed "
                    "Dedicated SQL Pool's %d-byte HEAP row limit.",
                    label, row_count, estimated_row_bytes, HEAP_MAX_ROW_BYTES,
                )
        create_ddl = build_create_table_sql(
            df, target_schema, target_table,
            conf.get("distribution", "ROUND_ROBIN"), conf.get("distribution_column"),
            structure, column_type_overrides,
        )
        logger.info("[%s] Target table not found - creating it:\n%s", label, create_ddl)
        # Guarded with IF NOT EXISTS: if a prior attempt's CREATE committed
        # server-side but execute_sql's own retry never saw the ack, a
        # retry here must no-op instead of failing on "already exists".
        execute_sql(
            f"IF NOT EXISTS (\n"
            f"    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id\n"
            f"    WHERE s.name = '{target_schema}' AND t.name = '{target_table}'\n"
            f")\n"
            f"BEGIN\n{create_ddl}\nEND"
        )
        if row_count == 0:
            return SyncResult(label, "CREATED_EMPTY", detail="target table created; source has no rows yet")
        load_target_table_fresh(df, target_schema, target_table)
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

    # --- Narrow the source read to rows not yet loaded, per the watermark --
    if FULL_REFRESH:
        logger.warning(
            "[%s] FULL_REFRESH is set - truncating the target and reloading the entire source "
            "(there's no business key to de-dup against, so this can't just re-stage on top).",
            label,
        )
        execute_sql(f"TRUNCATE TABLE {qualified(target_schema, target_table)}")
    else:
        max_watermark = query_scalar(
            f"SELECT MAX({quote_ident(watermark_column)}) FROM {qualified(target_schema, target_table)}"
        )
        if max_watermark is not None:
            watermark_type = dict((f.name, f.dataType) for f in df.schema.fields)[watermark_column]
            df = df.where(col(watermark_column) > lit(max_watermark).cast(watermark_type))
            logger.info("[%s] Watermark filter: %s > %s", label, watermark_column, max_watermark)

    candidate_count = df.limit(1).count()
    if candidate_count == 0:
        return SyncResult(label, "SKIPPED", detail="no candidate rows after watermark filter")

    candidate_count = df.count()
    logger.info("[%s] %d candidate row(s) to stage.", label, candidate_count)

    try:
        # Best-effort housekeeping only - must never fail the actual sync.
        try:
            stale_names = find_stale_staging_tables(
                staging_schema, f"{staging_table_prefix}_%", STALE_STAGING_TABLE_HOURS
            )
        except Exception as lookup_exc:
            logger.warning("[%s] Could not look up orphaned staging tables: %s", label, lookup_exc)
            stale_names = []
        for stale_name in stale_names:
            try:
                execute_sql(f"DROP TABLE {qualified(staging_schema, stale_name)}")
                logger.info("[%s] Dropped orphaned staging table from a previous run: %s", label, stale_name)
            except Exception as stale_exc:
                logger.warning("[%s] Could not drop orphaned staging table %s: %s", label, stale_name, stale_exc)

        load_staging_table(df, staging_schema, staging_table, target_column_defs)

        # No WHERE clause: the watermark filter above (or the FULL_REFRESH
        # truncate) already guarantees everything staged here is new -
        # there's no business key to re-check against on the way in.
        col_list = ", ".join(quote_ident(c) for c in target_columns)
        insert_sql = (
            f"INSERT INTO {qualified(target_schema, target_table)} ({col_list})\n"
            f"SELECT {col_list} FROM {qualified(staging_schema, staging_table)}"
        )
        inserted = execute_sql(insert_sql, timeout_seconds=1800)
        logger.info("[%s] Inserted %d new row(s).", label, inserted)
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
