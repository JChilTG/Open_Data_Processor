"""
Generate dedicated SQL pool CREATE TABLE DDL from a bronze profile.

Reads a profile's `schema` + `sql_pool` block and emits a `CREATE TABLE`
statement whose columns exactly match the projected DataFrame that
notebooks/bronze_publish.py writes (same drop_columns / rename), so you don't
have to hand-maintain the DDL per feed.

It prints the DDL and returns it via notebook.exit. If `output_path` is set, the
DDL is also written to that ADLS path.

Target runtime: Synapse Spark 3.5. Standalone notebook.
"""

from __future__ import annotations

import json
import re
import sys
import logging
from typing import Any, Dict, List, Optional

import yaml
from notebookutils import mssparkutils

# ===========================================================================
# PARAMETERS  (Synapse: mark this cell as the parameters cell)
# ===========================================================================
profile_name: str = ""
config_root: str = "config"
output_path: str = ""            # optional abfss:// path to write the .sql file
log_level: str = "INFO"


def get_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("generate_pool_ddl")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
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


# Spark/profile type -> dedicated SQL pool type.
_SIMPLE_POOL_TYPES = {
    "long": "BIGINT", "bigint": "BIGINT", "int": "INT", "integer": "INT",
    "short": "SMALLINT", "smallint": "SMALLINT", "byte": "TINYINT",
    "double": "FLOAT(53)", "float": "REAL", "bool": "BIT", "boolean": "BIT",
    "date": "DATE", "timestamp": "DATETIME2(6)",
}

# Source-file column, written right after the schema columns (before tokens).
_SOURCE_FILE_COLUMN = ("_source_file", "NVARCHAR(1024)")

# Bronze metadata columns (written after the token columns), with pool types.
_METADATA_COLUMNS: List[tuple] = [
    ("_bronze_loaded_at_utc_ts", "DATETIME2(6)"),
    ("_bronze_load_date", "DATE"),
    ("_bronze_profile_name", "VARCHAR(100)"),
    ("_bronze_batch_id", "VARCHAR(64)"),
    ("_bronze_run_id", "VARCHAR(64)"),
    ("_bronze_processor_version", "VARCHAR(20)"),
    ("_bronze_change_key", "CHAR(64)"),
]


def pool_type_for_schema_col(col_def: Dict[str, Any], default_len: int, max_len: int) -> str:
    if col_def.get("sql_type"):
        return str(col_def["sql_type"]).upper()
    yaml_type = str(col_def.get("type", "string")).strip().lower()
    base = yaml_type.split("(")[0].strip()
    if base in ("decimal", "numeric"):
        inner = re.search(r"\(([^)]*)\)", yaml_type)
        return f"DECIMAL({inner.group(1).replace(' ', '')})" if inner else "DECIMAL(18,0)"
    if base in _SIMPLE_POOL_TYPES:
        return _SIMPLE_POOL_TYPES[base]
    if base in ("string", "str"):
        length = col_def.get("sql_length", default_len)
        # Dedicated SQL pool has no NVARCHAR(MAX) here; sql_length: max maps to the
        # configured bound (max_string_length, <= 4000).
        if str(length).lower() == "max":
            length = max_len
        return f"NVARCHAR({int(length)})"
    raise ValueError(f"No pool type mapping for '{yaml_type}' (col {col_def.get('name')})")


def build_columns(cfg: Dict[str, Any]) -> List[tuple]:
    sp = cfg["sql_pool"]
    drop = set(sp.get("drop_columns") or [])
    rename = sp.get("rename") or {}
    default_len = int(sp.get("default_string_length", 4000))
    max_len = int(sp.get("max_string_length", 4000))

    cols: List[tuple] = []
    for c in cfg["schema"]:
        name = c["name"]
        if name in drop:
            continue
        cols.append((rename.get(name, name), pool_type_for_schema_col(c, default_len, max_len)))

    # _source_file is written right after the schema columns.
    sf_name, sf_type = _SOURCE_FILE_COLUMN
    if sf_name not in drop:
        cols.append((rename.get(sf_name, sf_name), sf_type))

    # Path-token columns (_token_<name>) sit between _source_file and metadata,
    # matching the column order written by bronze_ingest.py.
    template = (cfg.get("source", {}).get("path_template") or "").strip()
    token_len = int(sp.get("token_string_length", 256))
    for tok in re.findall(r"\{(\w+)\}", template):
        name = f"_token_{tok}"
        if name in drop:
            continue
        cols.append((rename.get(name, name), f"NVARCHAR({token_len})"))

    for name, pool_type in _METADATA_COLUMNS:
        if name in drop:
            continue
        cols.append((rename.get(name, name), pool_type))
    return cols


def build_ddl(cfg: Dict[str, Any]) -> str:
    sp = cfg["sql_pool"]
    schema = sp.get("schema", "bronze")
    table = sp.get("table") or cfg["target"]["table"]
    full = f"{schema}.{table}"

    cols = build_columns(cfg)
    width = max(len(f"[{n}]") for n, _ in cols)
    col_lines = ",\n".join(f"    {('[' + n + ']').ljust(width)}  {t}" for n, t in cols)

    distribution = str(sp.get("distribution", "ROUND_ROBIN")).upper()
    if distribution == "HASH":
        hash_col = sp.get("hash_column")
        if not hash_col:
            raise ValueError("sql_pool.hash_column is required when distribution = HASH")
        dist_clause = f"DISTRIBUTION = HASH([{hash_col}])"
    elif distribution == "REPLICATE":
        dist_clause = "DISTRIBUTION = REPLICATE"
    else:
        dist_clause = "DISTRIBUTION = ROUND_ROBIN"

    index_clause = str(sp.get("index", "CLUSTERED COLUMNSTORE INDEX"))

    return (
        f"-- Generated from profile '{cfg['profile_name']}'. Column order matches\n"
        f"-- the DataFrame written by bronze_publish.py.\n"
        f"CREATE TABLE {full}\n(\n{col_lines}\n)\n"
        f"WITH\n(\n    {dist_clause},\n    {index_clause}\n);\n"
    )


def main() -> dict:
    log = get_logger(log_level)
    cfg = load_config(config_root, profile_name)
    if not cfg.get("sql_pool"):
        raise ValueError(f"Profile '{profile_name}' has no sql_pool block.")
    ddl = build_ddl(cfg)
    log.info("Generated DDL:\n%s", ddl)
    if output_path:
        mssparkutils.fs.put(output_path, ddl, True)
        log.info("Wrote DDL to %s", output_path)
    return {"status": "success", "profile_name": cfg["profile_name"],
            "output_path": output_path or None, "ddl": ddl}


# ===========================================================================
# Entry point  (final notebook cell)
# ===========================================================================
mssparkutils.notebook.exit(json.dumps(main()))
