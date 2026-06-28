"""
Bronze ingestion processor for Azure Synapse Spark.

A single, fully parameterised, metadata-driven PySpark notebook that
incrementally ingests parquet / csv / delimited-txt files from a landing zone
into Delta bronze tables. Driven by one YAML *profile* per data feed.

Highlights
----------
- Incremental: only files not already loaded successfully are processed
  (tracked in the ``control.bronze_ingestion_log`` Delta table).
- Robust: every column is read as a string first, sanitised, then safely typed
  with ``try_cast``. Rows that fail parsing, typing or constraints are
  *quarantined* (never silently dropped) with a per-row reason.
- Reliable: optional file-readiness filter, quarantine circuit-breaker,
  idempotent Delta writes, and rich audit logging.
- Performant for large text files: explicit schema (no inference pass),
  ``multiLine=false`` so files stay splittable, tunable ``maxPartitionBytes``,
  and no Python UDFs (all native DataFrame / SQL expressions).

Target environment: Azure Synapse Spark 3.5 (open-source Delta Lake, no
Databricks). This file is structured as a Synapse notebook: the ``PARAMETERS``
block is the toggled parameters cell, and ``main()`` is the entry point invoked
at the bottom.
"""

from __future__ import annotations

import fnmatch
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import yaml  # PyYAML is available on Synapse Spark pools
from delta.tables import DeltaTable  # OSS Delta (Synapse Spark 3.5 / Delta 3.2)
from notebookutils import mssparkutils  # Synapse-provided utilities
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

PROCESSOR_VERSION = "1.0.0"
CORRUPT_COL = "_corrupt_record"
EXTRA_COLS_COL = "_bronze_extra_cols"   # JSON map of unexpected source columns

# ===========================================================================
# PARAMETERS  (Synapse: mark this cell as the parameters cell)
# ===========================================================================
profile_name: str = ""           # required: profile to run, e.g. "crm_customer"
config_root: str = "config"      # folder (lake path or local) with defaults.yaml + profiles/
batch_id: str = ""               # optional: supplied by the pipeline; auto-generated if blank
run_id: str = ""                 # optional: supplied by the pipeline; auto-generated if blank
log_level: str = "INFO"          # DEBUG | INFO | WARNING | ERROR
dry_run: bool = False            # validate + count only, write nothing
max_files_per_run: int = 0       # 0 = no cap
force_reprocess: bool = False    # ignore the control table and re-ingest matching files


# ===========================================================================
# Environment helpers
# ===========================================================================
def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("bronze_ingest")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        )
        logger.addHandler(handler)
    logger.propagate = False
    return logger


LOG = get_logger(log_level)


# ===========================================================================
# Config loading
# ===========================================================================
def read_text(path: str) -> str:
    """Read a UTF-8 text file from ADLS via mssparkutils."""
    return mssparkutils.fs.head(path, 16 * 1024 * 1024)


def join_path(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    return "/".join([base, *[p.strip("/") for p in parts]])


def _sql_lit(value: str) -> str:
    """Escape a string for safe inlining inside a single-quoted SQL literal."""
    return str(value).replace("'", "''")


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge ``override`` on top of ``base`` (override wins)."""
    out = dict(base)
    for key, val in (override or {}).items():
        if key in out and isinstance(out[key], dict) and isinstance(val, dict):
            out[key] = deep_merge(out[key], val)
        else:
            out[key] = val
    return out


def load_config(config_root: str, profile_name: str) -> Dict[str, Any]:
    if not profile_name:
        raise ValueError("Parameter 'profile_name' is required.")
    defaults = yaml.safe_load(read_text(join_path(config_root, "defaults.yaml"))) or {}
    profile = yaml.safe_load(
        read_text(join_path(config_root, "profiles", f"{profile_name}.yaml"))
    ) or {}
    cfg = deep_merge(defaults, profile)
    _validate_config(cfg)
    return cfg


def _validate_config(cfg: Dict[str, Any]) -> None:
    errors: List[str] = []
    if not cfg.get("profile_name"):
        errors.append("profile_name is missing")
    src = cfg.get("source", {})
    if src.get("format") not in ("txt", "csv", "parquet"):
        errors.append(f"unsupported or missing source.format: {src.get('format')}")
    if not src.get("path"):
        errors.append("source.path is missing")
    schema = cfg.get("schema") or []
    if not schema:
        errors.append("schema is missing or empty")
    tgt = cfg.get("target", {})
    if not tgt.get("table") or not tgt.get("path"):
        errors.append("target.table and target.path are required")
    if not cfg.get("quarantine", {}).get("path"):
        errors.append("quarantine.path is required")

    # Per-column validation: names, types, regex.
    seen: set = set()
    names: List[str] = []
    for i, c in enumerate(schema):
        name = c.get("name")
        if not name:
            errors.append(f"schema[{i}] is missing 'name'")
            continue
        names.append(name)
        if name in seen:
            errors.append(f"duplicate schema column: {name}")
        seen.add(name)
        if name.startswith("_bronze_") or name == "_source_file" or name.startswith("_token_"):
            errors.append(f"column '{name}' collides with a reserved processor column")
        try:
            to_sql_type(c.get("type", "string"))
        except ValueError as exc:
            errors.append(str(exc))
        if c.get("regex"):
            try:
                re.compile(c["regex"])
            except re.error as exc:
                errors.append(f"invalid regex for column '{name}': {exc}")

    schema_names = set(names)
    for b in (cfg.get("business_fields") or []):
        if b not in schema_names:
            errors.append(f"business_field '{b}' is not in schema")

    # partition_by must reference real columns (schema or processor metadata).
    valid_part = schema_names | {
        "_source_file", "_bronze_load_date", "_bronze_profile_name", "_bronze_batch_id",
    }
    for p in (tgt.get("partition_by") or []):
        if p not in valid_part:
            errors.append(f"target.partition_by '{p}' is not a known column")

    # dedicated-pool HASH distribution needs a hash_column that survives publish.
    sp = cfg.get("sql_pool") or {}
    if sp.get("enabled") and str(sp.get("distribution", "")).upper() == "HASH":
        hc = sp.get("hash_column")
        dropped = set(sp.get("drop_columns") or [])
        if not hc:
            errors.append("sql_pool.hash_column is required when distribution = HASH")
        elif hc in dropped:
            errors.append(f"sql_pool.hash_column '{hc}' is in drop_columns")

    if errors:
        raise ValueError("Invalid profile config:\n  - " + "\n  - ".join(errors))


# ===========================================================================
# Schema helpers
# ===========================================================================
_TYPE_RE = re.compile(r"^\s*[a-zA-Z]+\s*(\(\s*\d+\s*(,\s*\d+\s*)?\))?\s*$")
_TYPE_ALIASES = {
    "int": "int", "integer": "int", "long": "bigint", "bigint": "bigint",
    "short": "smallint", "smallint": "smallint", "byte": "tinyint",
    "string": "string", "str": "string", "double": "double", "float": "float",
    "bool": "boolean", "boolean": "boolean", "date": "date", "timestamp": "timestamp",
}


def to_sql_type(yaml_type: str) -> str:
    """Map a profile type name to a Spark SQL cast type string."""
    yaml_type = (yaml_type or "string").strip().lower()
    if not _TYPE_RE.match(yaml_type):
        raise ValueError(f"Unsupported type: {yaml_type}")
    base = yaml_type.split("(")[0].strip()
    if base in _TYPE_ALIASES:
        return _TYPE_ALIASES[base]
    if base in ("decimal", "numeric"):
        return yaml_type.replace("numeric", "decimal").replace(" ", "")
    raise ValueError(f"Unsupported type: {yaml_type}")


def is_string_type(yaml_type: str) -> bool:
    return to_sql_type(yaml_type) == "string"


def is_numeric_type(sql_type: str) -> bool:
    return sql_type.startswith(("decimal", "double", "float", "int", "bigint", "smallint", "tinyint"))


# ===========================================================================
# File discovery + incremental filtering
# ===========================================================================
@dataclass
class SourceFile:
    path: str
    size: int
    modified_ms: int


def list_files_recursive(path: str) -> List[SourceFile]:
    """Recursively list files under an ADLS path using mssparkutils."""
    results: List[SourceFile] = []
    stack = [path]
    while stack:
        current = stack.pop()
        for item in mssparkutils.fs.ls(current):
            if item.isDir:
                stack.append(item.path)
            else:
                results.append(SourceFile(
                    item.path, int(item.size), int(getattr(item, "modifyTime", 0))
                ))
    return results


def discover_files(cfg: Dict[str, Any], logger: logging.Logger) -> List[SourceFile]:
    src = cfg["source"]
    pattern = re.compile(fnmatch.translate(src.get("file_glob", "*")))
    all_files = list_files_recursive(src["path"])
    matched = [f for f in all_files if pattern.match(f.path.split("/")[-1])]
    logger.info("Discovered %d file(s); %d match glob %s",
                len(all_files), len(matched), src.get("file_glob", "*"))
    readiness = src.get("file_readiness") or {}
    if readiness.get("enabled"):
        matched = _apply_readiness_filter(matched, all_files, readiness, logger)
    return matched


def _apply_readiness_filter(matched, all_files, readiness, logger):
    marker = readiness.get("success_marker")
    min_age_min = int(readiness.get("min_file_age_minutes") or 0)
    now_ms = int(time.time() * 1000)
    marker_dirs = set()
    if marker:
        marker_dirs = {
            f.path.rsplit("/", 1)[0] for f in all_files
            if f.path.split("/")[-1] == marker
        }
    ready: List[SourceFile] = []
    for f in matched:
        if marker and f.path.rsplit("/", 1)[0] not in marker_dirs:
            logger.debug("Skipping %s: no %s marker in folder", f.path, marker)
            continue
        if min_age_min > 0 and f.modified_ms > 0:
            age_min = (now_ms - f.modified_ms) / 60000.0
            if age_min < min_age_min:
                logger.debug("Skipping %s: age %.1fm < %dm", f.path, age_min, min_age_min)
                continue
        ready.append(f)
    logger.info("Readiness filter: %d of %d file(s) ready", len(ready), len(matched))
    return ready


def filter_new_files(spark, cfg, files, logger) -> List[SourceFile]:
    if force_reprocess:
        logger.warning("force_reprocess=True: reprocessing %d matched file(s)", len(files))
        return _cap(files)
    control = cfg["control"]
    table = f"{control['database']}.{control['ingestion_log_table']}"
    if not spark.catalog.tableExists(table):
        logger.info("Control table %s missing; all files are new", table)
        return _cap(files)
    # Skip files already loaded successfully OR parked in dead_letter (exhausted
    # their retries) so we neither double-load nor retry hopeless files forever.
    done = {
        row["source_file"] for row in spark.sql(
            f"SELECT DISTINCT source_file FROM {table} "
            f"WHERE profile_name = '{_sql_lit(cfg['profile_name'])}' "
            f"AND status IN ('success', 'dead_letter')"
        ).collect()
    }
    new_files = [f for f in files if f.path not in done]
    logger.info("Incremental filter: %d new of %d matched file(s)", len(new_files), len(files))
    return _cap(new_files)


def _cap(files: List[SourceFile]) -> List[SourceFile]:
    if max_files_per_run and len(files) > max_files_per_run:
        return files[:max_files_per_run]
    return files


# ===========================================================================
# Reading
# ===========================================================================
def read_source(spark, cfg, files: List[SourceFile], logger,
                read_columns: Optional[List[str]] = None) -> DataFrame:
    """Read the source files as strings.

    ``read_columns`` overrides the set/order of columns to read (used when
    capturing extra source columns via schema_evolution=extra_cols_map). When
    ``None`` the declared profile schema order is used.
    """
    src = cfg["source"]
    fmt = src["format"]
    paths = [f.path for f in files]

    if fmt == "parquet":
        native = spark.read.parquet(*paths)
        names = read_columns if read_columns is not None else [c["name"] for c in cfg["schema"]]
        df = _select_columns_as_string(native, names)
        return df.withColumn(CORRUPT_COL, F.lit(None).cast("string"))

    names = read_columns if read_columns is not None else [c["name"] for c in cfg["schema"]]
    fields = [StructField(n, StringType(), True) for n in names]
    fields.append(StructField(CORRUPT_COL, StringType(), True))
    opt = src.get("options", {})
    reader = (
        spark.read.format("csv")
        .option("sep", opt.get("sep", ","))
        .option("header", str(bool(opt.get("header", True))).lower())
        .option("quote", opt.get("quote", '"'))
        .option("escape", opt.get("escape", '"'))
        .option("encoding", opt.get("encoding", "UTF-8"))
        .option("multiLine", str(bool(opt.get("multiLine", False))).lower())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .schema(StructType(fields))
    )
    if opt.get("comment"):
        reader = reader.option("comment", opt["comment"])
    logger.info("Reading %d file(s) with format=%s", len(paths), fmt)
    return reader.load(paths)


def get_actual_columns(spark, cfg, files: List[SourceFile], logger) -> Optional[List[str]]:
    """Best-effort list of the actual source column names (in file order).

    Returns None when columns cannot be determined (e.g. headerless CSV), in
    which case schema-drift detection is skipped.
    """
    if not files:
        return None
    fmt = cfg["source"]["format"]
    if fmt == "parquet":
        try:
            return list(spark.read.parquet(files[0].path).columns)
        except Exception as exc:
            logger.warning("Could not read parquet columns for drift detection: %s", exc)
            return None
    opt = cfg["source"].get("options", {})
    if not opt.get("header", True):
        return None  # positional file; named extra columns cannot be detected
    try:
        head = mssparkutils.fs.head(files[0].path, 256 * 1024)
        first_line = head.splitlines()[0] if head else ""
        sep = opt.get("sep", ",")
        quote = opt.get("quote", '"')
        return [h.strip().strip(quote) for h in first_line.split(sep)]
    except Exception as exc:
        logger.warning("Could not read header for drift detection: %s", exc)
        return None


def resolve_read_plan(spark, cfg, files, logger):
    """Decide the read column order and which source columns are unexpected.

    For headered CSV/TXT and parquet we detect the *actual* source columns and
    read in that order, so columns map by NAME rather than by position (a
    reordered or inserted source column no longer silently lands in the wrong
    field). Returns (read_columns, extras); ``read_columns`` is None only when
    columns cannot be detected (headerless CSV -> positional read). Raises
    RuntimeError when schema_evolution=fail and any drift is detected.
    """
    mode = cfg["quality"].get("schema_evolution", "extra_cols_map")
    declared = [c["name"] for c in cfg["schema"]]
    actual = get_actual_columns(spark, cfg, files, logger)
    if actual is None:
        return None, []

    extras = [a for a in actual if a not in declared]
    missing = [d for d in declared if d not in actual]
    if missing:
        logger.warning("Declared columns missing from source: %s", missing)
    if extras:
        logger.info("Extra source columns detected: %s", extras)
    if actual[:len(declared)] != declared:
        logger.info("Source column order differs from schema; aligning by name.")

    if mode == "fail" and (extras or missing):
        raise RuntimeError(
            f"schema_evolution=fail: drift detected (extra={extras}, missing={missing})"
        )
    return actual, extras


def conform_columns(df: DataFrame, cfg: Dict[str, Any], extras: List[str]) -> DataFrame:
    """Reconcile the read frame to the declared schema, handling extra columns.

    - extra_cols_map: pack unexpected columns into a JSON ``_bronze_extra_cols``
      column and drop them.
    - merge_schema: keep unexpected columns (Delta widens the bronze table).
    - any other mode: drop unexpected columns.
    Declared columns missing from the source are always added as null.
    """
    mode = cfg["quality"].get("schema_evolution", "extra_cols_map")
    present_extras = [e for e in extras if e in df.columns]

    if mode == "extra_cols_map":
        if present_extras:
            pairs: List[Column] = []
            for e in present_extras:
                pairs += [F.lit(e), F.col(e).cast("string")]
            df = df.withColumn(EXTRA_COLS_COL, F.to_json(F.create_map(*pairs))).drop(*present_extras)
        else:
            df = df.withColumn(EXTRA_COLS_COL, F.lit(None).cast("string"))
    elif mode == "merge_schema":
        pass  # keep extra columns; the Delta write widens the table
    elif present_extras:
        df = df.drop(*present_extras)

    for c in cfg["schema"]:
        if c["name"] not in df.columns:
            df = df.withColumn(c["name"], F.lit(None).cast("string"))
    return df


def _select_columns_as_string(df: DataFrame, names: List[str]) -> DataFrame:
    existing = set(df.columns)
    cols = []
    for name in names:
        if name in existing:
            cols.append(F.col(name).cast("string").alias(name))
        else:
            cols.append(F.lit(None).cast("string").alias(name))
    return df.select(*cols)


def path_template_to_regex(template: str, token_pattern: str) -> Tuple[str, List[str]]:
    """Convert a '{token}' path template into a regex with ordered capture groups.

    Returns (regex, [token_names]). Literal parts are regex-escaped; each
    ``{token}`` becomes a capture group matching ``token_pattern``.
    """
    parts = re.split(r"\{(\w+)\}", template)
    regex = ""
    tokens: List[str] = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            regex += re.escape(part)
        else:
            tokens.append(part)
            regex += f"({token_pattern})"
    return regex, tokens


def add_path_tokens(df: DataFrame, cfg: Dict[str, Any], logger) -> DataFrame:
    """Add ``_token_<name>`` columns extracted from each row's source file path."""
    template = (cfg["source"].get("path_template") or "").strip()
    if not template:
        return df
    token_pattern = cfg["source"].get("token_pattern", "[^/]+")
    regex, tokens = path_template_to_regex(template, token_pattern)
    if not tokens:
        logger.warning("path_template has no {tokens}: %s", template)
        return df
    for idx, name in enumerate(tokens, start=1):
        df = df.withColumn(f"_token_{name}", F.regexp_extract(F.col("_source_file"), regex, idx))
    logger.info("Path tokens added: %s", ", ".join(f"_token_{t}" for t in tokens))
    return df


def validate_headers(cfg, files, logger) -> None:
    """Best-effort, name-based header drift detection (warning only)."""
    if not cfg["parsing"].get("validate_headers"):
        return
    if cfg["source"]["format"] == "parquet":
        return
    if not cfg["source"].get("options", {}).get("header", True):
        return
    if not files:
        return
    expected = [c["name"] for c in cfg["schema"]]
    sep = cfg["source"]["options"].get("sep", ",")
    try:
        head = mssparkutils.fs.head(files[0].path, 64 * 1024)
        first_line = head.splitlines()[0] if head else ""
        actual = [h.strip().strip('"') for h in first_line.split(sep)]
        if actual != expected:
            logger.warning("Header drift in %s: expected %s but found %s",
                           files[0].path, expected, actual)
    except Exception as exc:  # best-effort only
        logger.debug("Header validation skipped: %s", exc)


# ===========================================================================
# Sanitisation + typing + validation
# ===========================================================================
def normalise_nulls(df: DataFrame, string_cols: List[str], null_values: List[str]) -> DataFrame:
    if not null_values:
        return df
    for col in string_cols:
        df = df.withColumn(
            col, F.when(F.col(col).isin(null_values), F.lit(None)).otherwise(F.col(col))
        )
    return df


def sanitise_strings(df: DataFrame, string_cols: List[str], san: Dict[str, Any]) -> DataFrame:
    if not san.get("enabled", True):
        return df
    control_re = r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]"
    bom_re = r"^[\ufeff\ufffe]"
    remove_chars = san.get("remove_chars") or []
    for col in string_cols:
        c = F.col(col)
        if san.get("strip_bom", True):
            c = F.regexp_replace(c, bom_re, "")
        if san.get("strip_control_chars", True):
            c = F.regexp_replace(c, control_re, "")
        for ch in remove_chars:
            c = F.regexp_replace(c, re.escape(ch), "")
        if san.get("normalise_whitespace", True):
            c = F.trim(F.regexp_replace(c, r"\s+", " "))
        df = df.withColumn(col, c)
    return df


def _clean_numeric(col: Column, parsing: Dict[str, Any]) -> Column:
    thousands = parsing.get("thousands_sep")
    if thousands:
        col = F.regexp_replace(col, re.escape(thousands), "")
    dec = parsing.get("decimal_sep", ".")
    if dec and dec != ".":
        col = F.regexp_replace(col, re.escape(dec), ".")
    return col


def apply_types_and_validate(df: DataFrame, cfg: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
    """Return (good, quarantine). Quarantine carries raw values + _reasons."""
    parsing = cfg["parsing"]
    schema = cfg["schema"]

    # Tier 1: rows the CSV parser could not split at all.
    parse_bad = (
        df.filter(F.col(CORRUPT_COL).isNotNull())
        .withColumn("_reasons", F.array(F.lit("parse_error")))
    )
    typed = df.filter(F.col(CORRUPT_COL).isNull())

    # Tier 2: safe typing + constraint checks.
    reason_cols: List[Column] = []
    for col_def in schema:
        name = col_def["name"]
        sql_type = to_sql_type(col_def["type"])
        typed = typed.withColumn(f"_raw_{name}", F.col(name))

        if sql_type == "string":
            typed_col = F.col(name)
        elif sql_type == "date":
            fmt = col_def.get("format") or parsing.get("date_format", "yyyy-MM-dd")
            typed_col = F.to_date(F.col(name), fmt)
        elif sql_type == "timestamp":
            fmt = col_def.get("format") or parsing.get("timestamp_format", "yyyy-MM-dd HH:mm:ss")
            typed_col = F.to_timestamp(F.col(name), fmt)
        else:
            src_col = _clean_numeric(F.col(name), parsing) if is_numeric_type(sql_type) else F.col(name)
            typed = typed.withColumn(f"_clean_{name}", src_col)
            typed_col = F.expr(f"try_cast(`_clean_{name}` AS {sql_type})")

        typed = typed.withColumn(name, typed_col)

        cast_failed = F.col(name).isNull() & F.col(f"_raw_{name}").isNotNull()
        reason_cols.append(F.when(cast_failed, F.lit(f"{name}:cast")))
        if col_def.get("required"):
            reason_cols.append(F.when(F.col(name).isNull(), F.lit(f"{name}:required")))
        if col_def.get("regex") and sql_type == "string":
            bad_regex = F.col(name).isNotNull() & ~F.col(name).rlike(col_def["regex"])
            reason_cols.append(F.when(bad_regex, F.lit(f"{name}:regex")))

    typed = typed.withColumn("_reasons_tmp", F.array(*reason_cols))
    typed = typed.withColumn("_reasons", F.expr("filter(_reasons_tmp, x -> x is not null)"))

    good = typed.filter(F.size("_reasons") == 0)
    type_bad = typed.filter(F.size("_reasons") > 0)

    drop_cols = [f"_raw_{c['name']}" for c in schema] + \
                [f"_clean_{c['name']}" for c in schema] + \
                ["_reasons_tmp", "_reasons", CORRUPT_COL]
    good = good.drop(*drop_cols)

    quarantine = parse_bad.unionByName(
        _type_bad_to_quarantine(type_bad, schema), allowMissingColumns=True
    )
    return good, _quarantine_columns(quarantine, schema)


def _type_bad_to_quarantine(df: DataFrame, schema) -> DataFrame:
    cols = [F.col(f"_raw_{c['name']}").alias(c["name"]) for c in schema]
    keep = cols + [F.col("_reasons"), F.col("_source_file")]
    return df.select(*keep)


def _quarantine_columns(df: DataFrame, schema) -> DataFrame:
    """Keep raw string columns, reasons, source file and the corrupt record."""
    ordered = [c["name"] for c in schema] + ["_reasons", "_source_file"]
    if CORRUPT_COL in df.columns:
        ordered.append(CORRUPT_COL)
    return df.select(*[c for c in ordered if c in df.columns])


# ===========================================================================
# Metadata + change key
# ===========================================================================
def add_metadata(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    # _source_file is already on the DataFrame; it is kept as-is. The remaining
    # processor-stamped columns use the _bronze_<field> convention.
    business = cfg.get("business_fields") or []
    out = (
        df.withColumn("_bronze_loaded_at_utc_ts", F.to_utc_timestamp(F.current_timestamp(), "UTC"))
        .withColumn("_bronze_load_date", F.to_date(F.col("_bronze_loaded_at_utc_ts")))
        .withColumn("_bronze_profile_name", F.lit(cfg["profile_name"]))
        .withColumn("_bronze_batch_id", F.lit(batch_id))
        .withColumn("_bronze_run_id", F.lit(run_id))
        .withColumn("_bronze_processor_version", F.lit(PROCESSOR_VERSION))
    )
    if business:
        change_cols = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in business]
        out = out.withColumn("_bronze_change_key", F.sha2(F.concat_ws("||", *change_cols), 256))
    else:
        out = out.withColumn("_bronze_change_key", F.lit(None).cast("string"))
    return _order_bronze_columns(out, cfg)


def bronze_column_order(cfg: Dict[str, Any]) -> List[str]:
    """Canonical bronze column order (kept in sync with the DDL/dbt generators)."""
    cols = [c["name"] for c in cfg["schema"]]
    cols.append("_source_file")
    template = (cfg["source"].get("path_template") or "").strip()
    if template:
        _regex, tokens = path_template_to_regex(template, cfg["source"].get("token_pattern", "[^/]+"))
        cols += [f"_token_{t}" for t in tokens]
    cols += ["_bronze_loaded_at_utc_ts", "_bronze_load_date", "_bronze_profile_name"]
    if cfg["quality"].get("schema_evolution") == "extra_cols_map":
        cols.append(EXTRA_COLS_COL)
    cols += ["_bronze_batch_id", "_bronze_run_id", "_bronze_processor_version", "_bronze_change_key"]
    return cols


def _order_bronze_columns(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    ordered = [c for c in bronze_column_order(cfg) if c in df.columns]
    extras = [c for c in df.columns if c not in ordered]
    return df.select(*ordered, *extras)


# ===========================================================================
# Writers
# ===========================================================================
def ensure_database(spark, database: str, base_path: Optional[str]) -> None:
    if base_path:
        spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {database} "
            f"LOCATION '{join_path(base_path, database)}'"
        )
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")


def write_bronze(spark, df: DataFrame, cfg: Dict[str, Any], logger):
    tgt = cfg["target"]
    ensure_database(spark, tgt["database"], cfg["control"].get("base_path"))
    path = tgt["path"]
    merge_schema = "true" if cfg["quality"]["schema_evolution"] == "merge_schema" else "false"

    # Idempotency: re-running the same batch_id (e.g. a pipeline retry) must not
    # duplicate rows. Delete any rows already present for this batch, then append.
    # This is correct across retries AND distinct batches, unlike a txnVersion
    # derived from a run count (which could collide or silently skip a write).
    if tgt.get("idempotent_writes", True) and DeltaTable.isDeltaTable(spark, path):
        logger.info("Idempotent write: removing any existing rows for batch_id=%s", batch_id)
        DeltaTable.forPath(spark, path).delete(F.col("_bronze_batch_id") == F.lit(batch_id))

    writer = df.write.format("delta").mode(tgt.get("write_mode", "append")) \
        .option("mergeSchema", merge_schema)
    if tgt.get("partition_by"):
        writer = writer.partitionBy(*tgt["partition_by"])
    writer.save(path)
    full_table = f"{tgt['database']}.{tgt['table']}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{tgt['path']}'")
    props = []
    if tgt.get("optimize_write", True):
        props.append("delta.autoOptimize.optimizeWrite = true")
    if tgt.get("auto_compact", True):
        props.append("delta.autoOptimize.autoCompact = true")
    if props:
        spark.sql(f"ALTER TABLE {full_table} SET TBLPROPERTIES ({', '.join(props)})")
    logger.info("Wrote bronze rows to %s (%s)", full_table, tgt["path"])


def write_quarantine(df: DataFrame, cfg: Dict[str, Any], logger):
    path = cfg["quarantine"]["path"]
    (
        df.withColumn("_bronze_quarantined_at_utc_ts", F.to_utc_timestamp(F.current_timestamp(), "UTC"))
        .withColumn("_bronze_profile_name", F.lit(cfg["profile_name"]))
        .withColumn("_bronze_batch_id", F.lit(batch_id))
        .withColumn("_bronze_run_id", F.lit(run_id))
        .withColumn("_bronze_processor_version", F.lit(PROCESSOR_VERSION))
        .write.format("delta").mode("append").option("mergeSchema", "true").save(path)
    )
    logger.info("Wrote quarantine rows to %s", path)


# ===========================================================================
# Control / audit logging
# ===========================================================================
# Explicit schemas so the control DataFrames never collapse a column to NullType
# (e.g. an all-null error_message). This keeps the Delta tables stable AND makes
# the rows safe to publish to the dedicated SQL pool. Columns/types match
# sql/control_tables.sql and sql/dedicated_pool_metrics_tables.sql.
_INGESTION_LOG_SCHEMA = StructType([
    StructField("profile_name", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("file_size_bytes", LongType(), True),
    StructField("file_modified_utc", TimestampType(), True),
    StructField("batch_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("rows_read", LongType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("quarantine_pct", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("attempt_count", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("started_at_utc", TimestampType(), True),
    StructField("ended_at_utc", TimestampType(), True),
    StructField("processor_version", StringType(), True),
])

_RUN_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("profile_name", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("files_discovered", IntegerType(), True),
    StructField("files_new", IntegerType(), True),
    StructField("files_processed", IntegerType(), True),
    StructField("files_failed", IntegerType(), True),
    StructField("rows_read", LongType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("quarantine_pct", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("started_at_utc", TimestampType(), True),
    StructField("ended_at_utc", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("processor_version", StringType(), True),
])


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def prior_attempt_counts(spark, cfg, paths: List[str]) -> Dict[str, int]:
    """How many times each file has already failed (failed/dead_letter rows)."""
    if not paths:
        return {}
    control = cfg["control"]
    table = f"{control['database']}.{control['ingestion_log_table']}"
    if not spark.catalog.tableExists(table):
        return {}
    rows = spark.sql(
        f"SELECT source_file, count(*) AS c FROM {table} "
        f"WHERE profile_name = '{_sql_lit(cfg['profile_name'])}' "
        f"AND status IN ('failed', 'dead_letter') GROUP BY source_file"
    ).collect()
    return {r["source_file"]: int(r["c"]) for r in rows}


def _append_log(spark, df, table, base_path):
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    if not spark.catalog.tableExists(table) and base_path:
        writer = writer.option("path", join_path(base_path, table.split(".")[-1]))
    writer.saveAsTable(table)


def log_files(spark, cfg, file_rows: List[Dict[str, Any]]):
    if not file_rows:
        return
    control = cfg["control"]
    ensure_database(spark, control["database"], control.get("base_path"))
    table = f"{control['database']}.{control['ingestion_log_table']}"
    df = spark.createDataFrame(file_rows, schema=_INGESTION_LOG_SCHEMA)
    _append_log(spark, df, table, control.get("base_path"))


def log_run(spark, cfg, run_row: Dict[str, Any]):
    control = cfg["control"]
    ensure_database(spark, control["database"], control.get("base_path"))
    table = f"{control['database']}.{control['run_log_table']}"
    df = spark.createDataFrame([run_row], schema=_RUN_LOG_SCHEMA)
    _append_log(spark, df, table, control.get("base_path"))


# ===========================================================================
# Per-load column profiling (null %, distinct, min/max) for trend/anomaly use
# ===========================================================================
_PROFILE_SCHEMA = StructType([
    StructField("profile_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("null_count", LongType(), True),
    StructField("null_pct", DoubleType(), True),
    StructField("distinct_count", LongType(), True),
    StructField("distinct_is_approx", BooleanType(), True),
    StructField("min_value", StringType(), True),
    StructField("max_value", StringType(), True),
    StructField("profiled_at_utc", TimestampType(), True),
    StructField("processor_version", StringType(), True),
])


def compute_column_profile(cfg: Dict[str, Any], good: DataFrame,
                           profiled_at: datetime, logger) -> List[Dict[str, Any]]:
    """Compute per-column metrics for the loaded rows in a single pass."""
    pcfg = cfg.get("profiling") or {}
    if not pcfg.get("enabled", True):
        return []
    type_by_name = {c["name"]: str(c.get("type", "string")) for c in cfg["schema"]}
    names = [n for n in (pcfg.get("columns") or list(type_by_name)) if n in good.columns]
    if not names:
        return []
    approx = bool(pcfg.get("approx_distinct", True))
    include_mm = bool(pcfg.get("include_min_max", True))

    aggs: List[Column] = [F.count(F.lit(1)).alias("__rows")]
    for n in names:
        aggs.append(F.count(F.when(F.col(n).isNull(), F.lit(1))).alias(f"{n}__nulls"))
        distinct = F.approx_count_distinct(F.col(n)) if approx else F.countDistinct(F.col(n))
        aggs.append(distinct.alias(f"{n}__distinct"))
        if include_mm:
            aggs.append(F.min(F.col(n)).alias(f"{n}__min"))
            aggs.append(F.max(F.col(n)).alias(f"{n}__max"))

    row = good.agg(*aggs).collect()[0]
    row_count = int(row["__rows"] or 0)
    table_name = cfg["target"]["table"]

    def _s(v: Any) -> Optional[str]:
        return None if v is None else str(v)

    rows: List[Dict[str, Any]] = []
    for n in names:
        nulls = int(row[f"{n}__nulls"] or 0)
        rows.append({
            "profile_name": cfg["profile_name"],
            "table_name": table_name,
            "run_id": run_id,
            "batch_id": batch_id,
            "column_name": n,
            "data_type": type_by_name.get(n, "string"),
            "row_count": row_count,
            "null_count": nulls,
            "null_pct": round(100.0 * nulls / row_count, 4) if row_count else 0.0,
            "distinct_count": int(row[f"{n}__distinct"] or 0),
            "distinct_is_approx": approx,
            "min_value": _s(row[f"{n}__min"]) if include_mm else None,
            "max_value": _s(row[f"{n}__max"]) if include_mm else None,
            "profiled_at_utc": profiled_at,
            "processor_version": PROCESSOR_VERSION,
        })
    logger.info("Profiled %d column(s) over %d row(s)", len(rows), row_count)
    return rows


def log_column_profile(spark, cfg, profile_rows: List[Dict[str, Any]]):
    if not profile_rows:
        return
    control = cfg["control"]
    ensure_database(spark, control["database"], control.get("base_path"))
    table = f"{control['database']}.{control.get('column_profile_table', 'bronze_column_profile')}"
    df = spark.createDataFrame(profile_rows, schema=_PROFILE_SCHEMA)
    _append_log(spark, df, table, control.get("base_path"))
    LOG.info("Wrote %d column-profile row(s) to %s", len(profile_rows), table)


# ===========================================================================
# Orchestration
# ===========================================================================
@dataclass
class RunResult:
    status: str
    files_discovered: int = 0
    files_new: int = 0
    files_processed: int = 0
    rows_read: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    quarantine_pct: float = 0.0
    error: str = ""


def main() -> Dict[str, Any]:
    global batch_id, run_id
    started = _now_utc()
    t0 = time.time()
    batch_id = batch_id or f"batch_{int(time.time())}"
    run_id = run_id or f"run_{int(time.time())}"

    spark = get_spark()
    cfg = load_config(config_root, profile_name)
    LOG.info("Loaded profile '%s' (processor v%s, dry_run=%s)",
             cfg["profile_name"], PROCESSOR_VERSION, dry_run)

    for key, val in (cfg.get("spark_conf") or {}).items():
        try:
            spark.conf.set(key, val)
        except Exception as exc:
            LOG.debug("Could not set %s: %s", key, exc)

    result = RunResult(status="success")
    files = discover_files(cfg, LOG)
    result.files_discovered = len(files)
    new_files = filter_new_files(spark, cfg, files, LOG)
    result.files_new = len(new_files)

    if not new_files:
        LOG.info("No new files to process; exiting as skipped.")
        result.status = "skipped"
        return _finish(spark, cfg, result, started, t0, [])

    validate_headers(cfg, new_files, LOG)

    try:
        read_columns, extras = resolve_read_plan(spark, cfg, new_files, LOG)
    except RuntimeError as exc:
        result.status = "schema_drift"
        result.error = str(exc)
        LOG.error(result.error)
        return _finish(spark, cfg, result, started, t0, [], failed_files=new_files)

    raw = read_source(spark, cfg, new_files, LOG, read_columns).withColumn(
        "_source_file", F.input_file_name()
    )
    raw = add_path_tokens(raw, cfg, LOG)
    raw = conform_columns(raw, cfg, extras)
    string_cols = [c["name"] for c in cfg["schema"] if is_string_type(c["type"])]
    raw = normalise_nulls(raw, string_cols, cfg["parsing"].get("null_values", []))
    raw = sanitise_strings(raw, string_cols, cfg.get("sanitisation", {}))

    # Cache the read+sanitised input and the two typed outputs so the file read,
    # sanitisation and typing happen once and are reused by the counts, the
    # writes and the profiling pass (instead of re-triggering the whole lineage
    # on every action). MEMORY_AND_DISK spills gracefully on large inputs.
    raw = raw.persist()
    cached: List[DataFrame] = [raw]
    try:
        good, quarantine = apply_types_and_validate(raw, cfg)
        good = add_metadata(good, cfg)
        if cfg["quality"].get("dedup", {}).get("on_change_key"):
            good = good.dropDuplicates(["_bronze_change_key"])
        good = good.persist()
        quarantine = quarantine.persist()
        cached += [good, quarantine]

        rows_loaded = good.count()
        rows_quarantined = quarantine.count()
        rows_read = rows_loaded + rows_quarantined
        result.rows_read, result.rows_loaded, result.rows_quarantined = rows_read, rows_loaded, rows_quarantined
        result.quarantine_pct = round(100.0 * rows_quarantined / rows_read, 2) if rows_read else 0.0
        LOG.info("Parsed %d rows: %d good, %d quarantined (%.2f%%)",
                 rows_read, rows_loaded, rows_quarantined, result.quarantine_pct)

        max_pct = cfg["quality"].get("max_quarantine_pct", 100)
        if rows_read and result.quarantine_pct > max_pct:
            result.status = "circuit_breaker"
            result.error = f"quarantine_pct {result.quarantine_pct}% exceeds max {max_pct}%"
            LOG.error(result.error)
            return _finish(spark, cfg, result, started, t0, [], failed_files=new_files)

        if dry_run:
            LOG.info("dry_run=True: nothing written.")
            result.status = "skipped"
            return _finish(spark, cfg, result, started, t0, [])

        if rows_quarantined > 0:
            write_quarantine(quarantine, cfg, LOG)
        if rows_loaded > 0:
            write_bronze(spark, good, cfg, LOG)
            profile_rows = compute_column_profile(cfg, good, _now_utc(), LOG)
            log_column_profile(spark, cfg, profile_rows)

        result.files_processed = len(new_files)
        return _finish(spark, cfg, result, started, t0, new_files)
    finally:
        for df in cached:
            try:
                df.unpersist()
            except Exception as exc:  # best-effort cleanup
                LOG.debug("unpersist failed: %s", exc)


def _finish(spark, cfg, result: RunResult, started, t0,
            processed_files: List[SourceFile], failed_files=None) -> Dict[str, Any]:
    ended = _now_utc()
    duration = round(time.time() - t0, 2)
    failed_statuses = ("circuit_breaker", "schema_drift", "failed")
    is_failure = result.status in failed_statuses

    files_for_log = failed_files or processed_files
    max_attempts = int(cfg["quality"].get("max_attempts", 3))
    # On failure, look up prior failures so we can bump attempt_count and park
    # files in dead_letter once they exhaust max_attempts (they are then skipped
    # by filter_new_files on subsequent runs instead of retrying forever).
    prior = prior_attempt_counts(spark, cfg, [f.path for f in files_for_log]) \
        if (is_failure and not dry_run) else {}

    file_rows = []
    for f in files_for_log:
        attempt = prior.get(f.path, 0) + 1
        if is_failure:
            status = "dead_letter" if attempt >= max_attempts else "failed"
        else:
            status = "success"
        file_rows.append({
            "profile_name": cfg["profile_name"],
            "source_file": f.path,
            "file_size_bytes": int(f.size),
            "file_modified_utc": datetime.utcfromtimestamp(f.modified_ms / 1000.0) if f.modified_ms else None,
            "batch_id": batch_id,
            "run_id": run_id,
            "rows_read": result.rows_read,
            "rows_loaded": result.rows_loaded,
            "rows_quarantined": result.rows_quarantined,
            "quarantine_pct": result.quarantine_pct,
            "status": status,
            "attempt_count": attempt,
            "error_message": result.error or None,
            "started_at_utc": started,
            "ended_at_utc": ended,
            "processor_version": PROCESSOR_VERSION,
        })

    if is_failure and file_rows:
        dead = sum(1 for r in file_rows if r["status"] == "dead_letter")
        if dead:
            LOG.error("%d file(s) reached max_attempts=%d and were marked dead_letter",
                      dead, max_attempts)

    try:
        if not dry_run and file_rows:
            log_files(spark, cfg, file_rows)
        if not dry_run:
            log_run(spark, cfg, {
                "run_id": run_id,
                "profile_name": cfg["profile_name"],
                "batch_id": batch_id,
                "files_discovered": result.files_discovered,
                "files_new": result.files_new,
                "files_processed": result.files_processed,
                "files_failed": len(failed_files or []),
                "rows_read": result.rows_read,
                "rows_loaded": result.rows_loaded,
                "rows_quarantined": result.rows_quarantined,
                "quarantine_pct": result.quarantine_pct,
                "status": result.status,
                "error_message": result.error or None,
                "started_at_utc": started,
                "ended_at_utc": ended,
                "duration_seconds": duration,
                "processor_version": PROCESSOR_VERSION,
            })
    except Exception as exc:
        LOG.error("Failed to write control/audit logs: %s", exc)

    metrics = {
        "status": result.status,
        "profile_name": cfg["profile_name"],
        "batch_id": batch_id,
        "run_id": run_id,
        "files_discovered": result.files_discovered,
        "files_new": result.files_new,
        "files_processed": result.files_processed,
        "rows_read": result.rows_read,
        "rows_loaded": result.rows_loaded,
        "rows_quarantined": result.rows_quarantined,
        "quarantine_pct": result.quarantine_pct,
        "duration_seconds": duration,
        "error": result.error,
    }
    LOG.info("Run complete: %s", json.dumps(metrics))
    if result.status in ("circuit_breaker", "schema_drift", "failed"):
        raise RuntimeError(result.error)
    return metrics


# ===========================================================================
# Entry point  (final notebook cell)
# ===========================================================================
mssparkutils.notebook.exit(json.dumps(main()))
