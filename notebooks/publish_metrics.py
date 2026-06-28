"""
Publish run metrics + column profiles -> Azure Synapse Dedicated SQL Pool.

Reads the control Delta tables written by notebooks/bronze_ingest.py
(``control.bronze_run_log`` and ``control.bronze_column_profile``) and bulk-loads
the rows for a run / batch (or everything) into matching dedicated SQL pool
tables using the **Azure Synapse Dedicated SQL Pool Connector for Apache Spark**
(`com.microsoft.spark.sqlanalytics`). COPY + managed-identity staging; no
external tables, no serverless.

This lets dbt / Power BI trend load volumes, quarantine rates, null %, distinct
counts and min/max over time for anomaly detection. Pre-create the destination
tables (see sql/dedicated_pool_metrics_tables.sql).

Target runtime: Synapse Spark 3.5 / Delta Lake 3.2. Standalone notebook.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict, List

import yaml
from notebookutils import mssparkutils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

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
run_id: str = ""                 # publish metrics for this run_id
batch_id: str = ""               # or for this batch_id
log_level: str = "INFO"
publish_all: bool = False        # publish every metrics row for the profile


def get_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("publish_metrics")
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


def _sql_lit(value: str) -> str:
    return str(value).replace("'", "''")


def pool_execute(spark, sp: Dict[str, Any], database: str, sql: str, logger: logging.Logger) -> None:
    """Run a T-SQL statement on the dedicated pool over JDBC (AAD MSI token)."""
    server = sp.get("server")
    if not server:
        raise ValueError(
            "sql_pool.server (e.g. <workspace>.sql.azuresynapse.net) is required "
            "for idempotent metrics publish; set it or disable metrics_publish.idempotent."
        )
    token = mssparkutils.credentials.getToken("DW")
    jvm = spark.sparkContext._jvm
    url = (
        f"jdbc:sqlserver://{server}:1433;database={database};"
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


def _selected(df: DataFrame) -> DataFrame:
    """Apply the run_id / batch_id / publish_all selection to a metrics frame."""
    df = df.where(F.col("profile_name") == profile_name)
    if publish_all:
        return df
    if run_id:
        return df.where(F.col("run_id") == run_id)
    if batch_id:
        return df.where(F.col("batch_id") == batch_id)
    raise ValueError("Provide run_id or batch_id, or set publish_all=True.")


def _publish(df: DataFrame, dest: str, mp: Dict[str, Any], sp: Dict[str, Any],
             logger: logging.Logger) -> int:
    count = df.count()
    if count == 0:
        logger.info("No rows for %s; skipping.", dest)
        return 0
    temp_folder = sp.get("temp_folder")
    if not temp_folder:
        raise ValueError("sql_pool.temp_folder is required for the COPY staging area.")
    writer = df.write.mode(mp.get("mode", "append"))
    if sp.get("server"):
        writer = writer.option(SERVER_OPT, sp["server"])
    logger.info("Publishing %d row(s) to %s (mode=%s)", count, dest, mp.get("mode", "append"))
    writer.synapsesql(dest, INTERNAL_TABLE, temp_folder)
    return count


def main() -> dict:
    log = get_logger(log_level)
    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(config_root, profile_name)

    mp = cfg.get("metrics_publish") or {}
    if not mp.get("enabled"):
        log.info("metrics_publish.enabled is false for '%s'; nothing to publish.", profile_name)
        return {"status": "skipped", "reason": "metrics_publish disabled"}

    sp = cfg.get("sql_pool") or {}
    database = mp.get("database") or sp.get("database")
    if not database:
        raise ValueError("metrics_publish.database or sql_pool.database is required.")
    schema = mp.get("schema", "operations")
    control = cfg["control"]

    published: Dict[str, int] = {}
    sources = [
        (control["run_log_table"], mp.get("run_log_table", "bronze_run_log")),
        (control.get("column_profile_table", "bronze_column_profile"),
         mp.get("column_profile_table", "bronze_column_profile")),
    ]
    # Idempotency: delete the rows we're about to (re)publish first, so reruns of
    # the same run_id / batch_id don't duplicate metrics in the pool. Skipped for
    # publish_all (a backfill).
    idempotent = mp.get("idempotent") and not publish_all
    if idempotent and not (run_id or batch_id):
        raise ValueError("Idempotent metrics publish needs run_id or batch_id (or set publish_all).")
    sel_col, sel_val = ("run_id", run_id) if run_id else ("batch_id", batch_id)

    for src_table, dest_table in sources:
        full_src = f"{control['database']}.{src_table}"
        if not spark.catalog.tableExists(full_src):
            log.warning("Source table %s does not exist; skipping.", full_src)
            continue
        df = _selected(spark.table(full_src))
        dest = f"{schema}.{dest_table}"
        if idempotent:
            pool_execute(
                spark, sp, database,
                f"IF OBJECT_ID('{dest}') IS NOT NULL DELETE FROM {dest} "
                f"WHERE profile_name = '{_sql_lit(profile_name)}' "
                f"AND {sel_col} = '{_sql_lit(sel_val)}'",
                log,
            )
        published[f"{database}.{dest}"] = _publish(df, f"{database}.{dest}", mp, sp, log)

    metrics = {
        "status": "success",
        "profile_name": profile_name,
        "run_id": run_id or None,
        "batch_id": batch_id or None,
        "published": published,
        "rows_published": sum(published.values()),
    }
    log.info("Metrics publish complete: %s", json.dumps(metrics))
    return metrics


# ===========================================================================
# Entry point  (final notebook cell)
# ===========================================================================
mssparkutils.notebook.exit(json.dumps(main()))
