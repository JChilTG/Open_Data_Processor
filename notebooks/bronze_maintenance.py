"""
Bronze Delta maintenance for Azure Synapse Spark 3.5.

Runs OPTIMIZE (with optional ZORDER) and VACUUM against a bronze Delta table to
keep it performant for downstream dbt reads and to control storage / the
time-travel window. Intended to run on a schedule (e.g. nightly) per profile.

Standalone: it reads the same YAML profiles as the ingestion notebook but does
not import it (Synapse notebooks are not importable as modules).
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict, List

import yaml
from notebookutils import mssparkutils
from pyspark.sql import SparkSession

# ===========================================================================
# PARAMETERS  (Synapse: mark this cell as the parameters cell)
# ===========================================================================
profile_name: str = ""
config_root: str = "config"
log_level: str = "INFO"
zorder_by: str = ""                 # comma-separated columns, e.g. "customer_id"
vacuum_retain_hours: int = 168      # 7 days; set deliberately (affects time travel)
run_vacuum: bool = True
run_optimize: bool = True
allow_low_retention: bool = False   # permit VACUUM RETAIN < 168h (disables the safety check)


def get_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("bronze_maintenance")
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


def main() -> dict:
    log = get_logger(log_level)
    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(config_root, profile_name)
    tgt = cfg["target"]
    full_table = f"{tgt['database']}.{tgt['table']}"
    log.info("Maintenance for %s (%s)", full_table, tgt["path"])

    if not spark.catalog.tableExists(full_table):
        log.warning("Table %s does not exist; nothing to do.", full_table)
        return {"status": "skipped", "table": full_table}

    if run_optimize:
        sql = f"OPTIMIZE {full_table}"
        cols: List[str] = [c.strip() for c in zorder_by.split(",") if c.strip()]
        if cols:
            sql += f" ZORDER BY ({', '.join(cols)})"
        log.info("Running: %s", sql)
        spark.sql(sql)

    retain = int(vacuum_retain_hours)
    retention_check_disabled = False
    if run_vacuum:
        # Delta refuses VACUUM with RETAIN < 168h (it can corrupt concurrent
        # readers / break time travel) unless the safety check is disabled.
        if retain < 168:
            if not allow_low_retention:
                log.warning(
                    "vacuum_retain_hours=%d < 168; clamping to 168. Set "
                    "allow_low_retention=true to force a shorter window.", retain)
                retain = 168
            else:
                log.warning("Forcing VACUUM with RETAIN %d HOURS (< 168); "
                            "disabling Delta retention-duration check.", retain)
                spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                retention_check_disabled = True
        try:
            sql = f"VACUUM {full_table} RETAIN {retain} HOURS"
            log.info("Running: %s", sql)
            spark.sql(sql)
        finally:
            if retention_check_disabled:
                spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

    return {"status": "success", "table": full_table,
            "optimized": run_optimize, "vacuumed": run_vacuum,
            "vacuum_retain_hours": retain if run_vacuum else None}


# ===========================================================================
# Entry point  (final notebook cell)
# ===========================================================================
mssparkutils.notebook.exit(json.dumps(main()))
