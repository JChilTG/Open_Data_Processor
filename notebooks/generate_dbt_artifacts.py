"""
Generate dbt sources.yml and staging models from bronze profiles.

Reads one or more profile YAML files, applies config/defaults.yaml, and emits:

- sources.yml describing the dedicated SQL pool bronze source tables.
- One staging model SQL file per profile, selecting from source(...).

The generated column list matches notebooks/bronze_publish.py output:
source schema columns, _source_file, optional _token_* columns, then _bronze_*
metadata columns, with sql_pool.drop_columns / sql_pool.rename applied.

Target runtime: Synapse Spark 3.5. Standalone notebook.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any, Dict, Iterable, List, Tuple

import yaml
from notebookutils import mssparkutils

# ===========================================================================
# PARAMETERS  (Synapse: mark this cell as the parameters cell)
# ===========================================================================
profile_name: str = ""       # optional single profile; blank = all profiles under config_root/profiles
config_root: str = "config"
output_root: str = ""        # optional; defaults to dbt.output_root, else prints only
log_level: str = "INFO"


def get_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("generate_dbt_artifacts")
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


def _read_yaml(path: str) -> Dict[str, Any]:
    return yaml.safe_load(mssparkutils.fs.head(path, 16 * 1024 * 1024)) or {}


def load_defaults(config_root: str) -> Dict[str, Any]:
    return _read_yaml(_join_path(config_root, "defaults.yaml"))


def load_profile(config_root: str, defaults: Dict[str, Any], name: str) -> Dict[str, Any]:
    profile = _read_yaml(_join_path(config_root, "profiles", f"{name}.yaml"))
    return _deep_merge(defaults, profile)


def list_profile_names(config_root: str) -> List[str]:
    return sorted(
        item.name[:-5]
        for item in mssparkutils.fs.ls(_join_path(config_root, "profiles"))
        if not item.isDir and item.name.endswith(".yaml")
    )


_SIMPLE_DBT_TYPES = {
    "long": "bigint", "bigint": "bigint", "int": "integer", "integer": "integer",
    "short": "smallint", "smallint": "smallint", "byte": "tinyint",
    "double": "float", "float": "real", "bool": "boolean", "boolean": "boolean",
    "date": "date", "timestamp": "datetime2", "string": "nvarchar", "str": "nvarchar",
}


def dbt_type_for_col(col_def: Dict[str, Any], default_len: int, max_len: int) -> str:
    if col_def.get("sql_type"):
        return str(col_def["sql_type"]).lower()
    yaml_type = str(col_def.get("type", "string")).strip().lower()
    base = yaml_type.split("(")[0].strip()
    if base in ("decimal", "numeric"):
        return yaml_type.replace("numeric", "decimal")
    if base in ("string", "str"):
        length = col_def.get("sql_length", default_len)
        # No NVARCHAR(MAX) in this pool; sql_length: max maps to the configured bound.
        if str(length).lower() == "max":
            length = max_len
        return f"nvarchar({int(length)})"
    return _SIMPLE_DBT_TYPES.get(base, yaml_type)


def published_columns(cfg: Dict[str, Any]) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Return (column_name, data_type, source_column_definition) in publish order."""
    sp = cfg["sql_pool"]
    drop = set(sp.get("drop_columns") or [])
    rename = sp.get("rename") or {}
    default_len = int(sp.get("default_string_length", 4000))
    max_len = int(sp.get("max_string_length", 4000))
    token_len = int(sp.get("token_string_length", 256))

    cols: List[Tuple[str, str, Dict[str, Any]]] = []
    for col_def in cfg["schema"]:
        name = col_def["name"]
        if name not in drop:
            cols.append((rename.get(name, name), dbt_type_for_col(col_def, default_len, max_len), col_def))

    if "_source_file" not in drop:
        cols.append((rename.get("_source_file", "_source_file"), "nvarchar(1024)", {}))

    template = (cfg.get("source", {}).get("path_template") or "").strip()
    for token in re.findall(r"\{(\w+)\}", template):
        name = f"_token_{token}"
        if name not in drop:
            cols.append((rename.get(name, name), f"nvarchar({token_len})", {}))

    metadata = [
        ("_bronze_loaded_at_utc_ts", "datetime2"),
        ("_bronze_load_date", "date"),
        ("_bronze_profile_name", "varchar(100)"),
        ("_bronze_batch_id", "varchar(64)"),
        ("_bronze_run_id", "varchar(64)"),
        ("_bronze_processor_version", "varchar(20)"),
        ("_bronze_change_key", "char(64)"),
    ]
    for name, dtype in metadata:
        if name not in drop:
            cols.append((rename.get(name, name), dtype, {}))
    return cols


def build_sources_yml(configs: Iterable[Dict[str, Any]]) -> str:
    configs = [c for c in configs if c.get("dbt", {}).get("enabled", True)]
    if not configs:
        raise ValueError("No dbt-enabled profiles found.")

    dbt_cfg = configs[0].get("dbt", {})
    source_name = dbt_cfg.get("source_name", "bronze")
    source = {
        "name": source_name,
        "description": dbt_cfg.get("source_description", "Bronze source tables."),
        "schema": configs[0]["sql_pool"].get("schema", "bronze"),
        "tables": [],
    }
    database = configs[0]["sql_pool"].get("database")
    if database:
        source["database"] = database

    include_types = bool(dbt_cfg.get("include_column_data_types", True))
    include_not_null = bool(dbt_cfg.get("include_not_null_tests", True))
    include_change_key_test = bool(dbt_cfg.get("include_change_key_test", False))

    for cfg in configs:
        sp = cfg["sql_pool"]
        table_name = sp.get("table") or cfg["target"]["table"]
        table = {
            "name": table_name,
            "description": f"Bronze table generated from profile {cfg['profile_name']}.",
            "columns": [],
        }
        if sp.get("schema") and sp.get("schema") != source["schema"]:
            table["schema"] = sp["schema"]
        if sp.get("database") and sp.get("database") != source.get("database"):
            table["database"] = sp["database"]

        required_cols = {c["name"] for c in cfg["schema"] if c.get("required")}
        for name, dtype, col_def in published_columns(cfg):
            column = {"name": name}
            if include_types:
                column["data_type"] = dtype
            tests = []
            if include_not_null and name in required_cols:
                tests.append("not_null")
            if include_change_key_test and name == "_bronze_change_key":
                tests.append("not_null")
            if tests:
                column["tests"] = tests
            table["columns"].append(column)
        source["tables"].append(table)

    return yaml.safe_dump({"version": 2, "sources": [source]}, sort_keys=False, width=120)


def bracket_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def build_staging_sql(cfg: Dict[str, Any]) -> str:
    dbt_cfg = cfg.get("dbt", {})
    source_name = dbt_cfg.get("source_name", "bronze")
    model_materialized = dbt_cfg.get("staging_materialized", "view")
    table_name = cfg["sql_pool"].get("table") or cfg["target"]["table"]
    cols = [name for name, _dtype, _defn in published_columns(cfg)]
    select_list = ",\n    ".join(bracket_ident(c) for c in cols)
    return (
        "{{ config(materialized='" + model_materialized + "') }}\n\n"
        f"select\n    {select_list}\n"
        f"from {{{{ source('{source_name}', '{table_name}') }}}}\n"
    )


def model_filename(cfg: Dict[str, Any]) -> str:
    prefix = cfg.get("dbt", {}).get("staging_model_prefix", "stg_")
    return f"{prefix}{cfg['profile_name']}.sql"


def write_outputs(root: str, sources_yml: str, models: Dict[str, str]) -> List[str]:
    written = []
    sources_path = _join_path(root, "models", "bronze", "sources.yml")
    mssparkutils.fs.put(sources_path, sources_yml, True)
    written.append(sources_path)
    for filename, sql in models.items():
        path = _join_path(root, "models", "staging", filename)
        mssparkutils.fs.put(path, sql, True)
        written.append(path)
    return written


def main() -> dict:
    log = get_logger(log_level)
    defaults = load_defaults(config_root)
    names = [profile_name] if profile_name else list_profile_names(config_root)
    configs = [load_profile(config_root, defaults, n) for n in names]
    configs = [c for c in configs if c.get("dbt", {}).get("enabled", True)]
    if not configs:
        raise ValueError("No dbt-enabled profiles to generate.")

    sources_yml = build_sources_yml(configs)
    models = {model_filename(c): build_staging_sql(c) for c in configs}
    root = output_root or configs[0].get("dbt", {}).get("output_root") or ""

    log.info("Generated sources.yml:\n%s", sources_yml)
    for name, sql in models.items():
        log.info("Generated model %s:\n%s", name, sql)

    written = write_outputs(root, sources_yml, models) if root else []
    if written:
        log.info("Wrote %d dbt artifact(s) under %s", len(written), root)

    return {
        "status": "success",
        "profiles": [c["profile_name"] for c in configs],
        "sources_yml": sources_yml,
        "models": models,
        "written": written,
    }


# ===========================================================================
# Entry point  (final notebook cell)
# ===========================================================================
mssparkutils.notebook.exit(json.dumps(main()))
