"""
Publish bronze Delta -> Azure Synapse Dedicated SQL Pool.

Reads a bronze Delta table (the rows from one ingest batch, or the whole table)
and bulk-loads them into a dedicated SQL pool table using the **Azure Synapse
Dedicated SQL Pool Connector for Apache Spark** (`com.microsoft.spark.sqlanalytics`).

The connector loads via the `COPY` command with managed-identity ADLS staging.
No external tables, no serverless. Pre-create the destination table with the
desired DISTRIBUTION + CLUSTERED COLUMNSTORE INDEX (see
sql/dedicated_pool_tables.sql) for best downstream dbt performance.

Target runtime: Synapse Spark 3.5 / Delta Lake 3.2. Standalone notebook (does
not import the other notebooks).
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict

import yaml
from notebookutils import mssparkutils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# The connector's table-type constant. Import defensively across connector
# versions; the value is "internal" (load into a managed/internal pool table).
try:
    from com.microsoft.spark.sqlanalytics.utils.Constants import Constants  # type: ignore
    INTERNAL_TABLE = Constants.INTERNAL
    SERVER_OPT = Constants.SERVER
except Exception:  # pragma: no cover - resolved on the Synapse pool
    INTERNAL_TABLE = "internal"
    SERVER_OPT = "Constants.SERVER"

# ===========================================================================
# PARAMETERS  (Synapse: mark this cell as the parameters cell)
# ===========================================================================
profile_name: str = ""
config_root: str = "config"
batch_id: str = ""               # publish rows with this _batch_id (from the ingest run)
log_level: str = "INFO"
publish_all: bool = False        # publish the entire bronze table (ignores batch_id)


def get_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("bronze_publish")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        )
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def _join_path(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    return "/".join([base, *[p.strip("/") for p in parts]])


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, val in (override or {}).items():
        if key in out and isinstance(out[key], dict) and isinstance(val, dict):
            out[key] = _deep_merge(out[key], val)
        else:
            out[key] = val
    return out


def load_config(config_root: str, profile_name: str) -> Dict[str, Any]:
    if not profile_name:
        raise ValueError("Parameter 'profile_name' is required.")
    defaults = yaml.safe_load(
        mssparkutils.fs.head(_join_path(config_root, "defaults.yaml"), 16 * 1024 * 1024)
    ) or {}
    profile = yaml.safe_load(
        mssparkutils.fs.head(
            _join_path(config_root, "profiles", f"{profile_name}.yaml"), 16 * 1024 * 1024
        )
    ) or {}
    return _deep_merge(defaults, profile)


def project_for_pool(df: DataFrame, sp: Dict[str, Any]) -> DataFrame:
    """Drop internal columns and rename underscore-prefixed columns."""
    drop_cols = [c for c in (sp.get("drop_columns") or []) if c in df.columns]
    if drop_cols:
        df = df.drop(*drop_cols)
    for old, new in (sp.get("rename") or {}).items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def _sql_lit(value: str) -> str:
    return str(value).replace("'", "''")


def pool_execute(spark, sp: Dict[str, Any], sql: str, logger: logging.Logger) -> None:
    """Run a T-SQL statement on the dedicated pool over JDBC (AAD MSI token).

    Used for idempotent pre-deletes. Requires sql_pool.server (the SQL endpoint,
    e.g. <workspace>.sql.azuresynapse.net) and the workspace managed identity to
    have rights on the pool.
    """
    server = sp.get("server")
    if not server:
        raise ValueError(
            "sql_pool.server (e.g. <workspace>.sql.azuresynapse.net) is required "
            "for idempotent publish; set it or disable sql_pool.idempotent."
        )
    token = mssparkutils.credentials.getToken("DW")
    jvm = spark.sparkContext._jvm
    url = (
        f"jdbc:sqlserver://{server}:1433;database={sp['database']};"
        "encrypt=true;trustServerCertificate=false;"
        "hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30"
    )
    props = jvm.java.util.Properties()
    props.setProperty("accessToken", token)
    logger.info("Pool pre-delete: %s", sql)
    conn = jvm.java.sql.DriverManager.getConnection(url, props)
    try:
        conn.createStatement().execute(sql)
    finally:
        conn.close()


def publish(df: DataFrame, sp: Dict[str, Any], logger: logging.Logger) -> None:
    table = sp.get("table")
    dest = f"{sp['database']}.{sp['schema']}.{table}"
    temp_folder = sp.get("temp_folder")
    if not temp_folder:
        raise ValueError("sql_pool.temp_folder is required for the COPY staging area.")

    writer = df.write.mode(sp.get("mode", "append"))
    if sp.get("server"):  # only needed for a pool in a different workspace
        writer = writer.option(SERVER_OPT, sp["server"])

    logger.info("Publishing to dedicated pool table %s (mode=%s)", dest, sp.get("mode", "append"))
    writer.synapsesql(dest, INTERNAL_TABLE, temp_folder)


def main() -> dict:
    log = get_logger(log_level)
    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(config_root, profile_name)

    sp = cfg.get("sql_pool") or {}
    if not sp.get("enabled"):
        log.info("sql_pool.enabled is false for profile '%s'; nothing to publish.", profile_name)
        return {"status": "skipped", "reason": "sql_pool disabled"}
    if not sp.get("database"):
        raise ValueError("sql_pool.database is required.")
    sp.setdefault("table", cfg["target"]["table"])
    sp.setdefault("schema", "bronze")

    bronze_path = cfg["target"]["path"]
    df = spark.read.format("delta").load(bronze_path)

    if publish_all:
        log.info("publish_all=True: publishing entire bronze table.")
    elif batch_id:
        df = df.where(F.col("_bronze_batch_id") == batch_id)
        log.info("Publishing rows for _bronze_batch_id=%s", batch_id)
    else:
        raise ValueError("Provide a batch_id, or set publish_all=True.")

    df = project_for_pool(df, sp)
    row_count = df.count()
    if row_count == 0:
        log.info("No rows to publish; exiting.")
        return {"status": "skipped", "reason": "no rows", "rows_published": 0}

    # Idempotency: remove any rows already published for this batch before the
    # append, so a pipeline retry doesn't duplicate rows in the pool. No-ops if
    # the table doesn't exist yet (IF OBJECT_ID ...).
    if sp.get("idempotent") and not publish_all and batch_id:
        batch_col = (sp.get("rename") or {}).get("_bronze_batch_id", "_bronze_batch_id")
        dest = f"{sp['schema']}.{sp['table']}"
        pool_execute(
            spark, sp,
            f"IF OBJECT_ID('{dest}') IS NOT NULL "
            f"DELETE FROM {dest} WHERE [{batch_col}] = '{_sql_lit(batch_id)}'",
            log,
        )

    publish(df, sp, log)

    metrics = {
        "status": "success",
        "profile_name": profile_name,
        "batch_id": batch_id,
        "destination": f"{sp['database']}.{sp['schema']}.{sp['table']}",
        "mode": sp.get("mode", "append"),
        "rows_published": row_count,
    }
    log.info("Publish complete: %s", json.dumps(metrics))
    return metrics


# ===========================================================================
# Entry point  (final notebook cell)
# ===========================================================================
mssparkutils.notebook.exit(json.dumps(main()))
