# Synapse Dedicated SQL Pool version

A T-SQL port of the SCD2 macro + models so they run on **Azure Synapse Analytics
Dedicated SQL Pool**. These files live outside the local project's `models/` and
`macros/` paths on purpose — the learning project compiles against DuckDB, and
this code is meant to be copied into a real [`dbt-synapse`](https://github.com/dbt-msft/dbt-synapse)
project.

## Contents

```
synapse/
  macros/hash_all_except.sql              chunked HASHBYTES hash (scales past 254 cols)
  macros/create_statistics.sql            idempotent CREATE STATISTICS (post-hook)
  models/staging/stg_..._core.sql         typed/cleaned columns (view)
  models/staging/stg_..._snapshots.sql    adds attribute_hash via the macro (view)
  models/marts/dim_account_scd2.sql       SCD2 dimension (REPLICATE table)
  tests/                                  singular tests, adapted for BIT/T-SQL
```

## create_statistics

Synapse does not auto-create statistics, and missing/stale stats are a top cause
of slow models. Use this macro as a post-hook to (re)create stats on join keys,
filter columns, and distribution keys after a model builds. It is idempotent
(guards with `sys.stats`), supports single and composite stats, and accepts
`scan='fullscan'` or `scan=<N>` for `WITH SAMPLE N PERCENT`.

```sql
{{ config(
    materialized='table',
    dist='HASH(account_id)',
    post_hook="{{ create_statistics(['account_id', 'snapshot_date', ['region','industry']]) }}"
) }}
```

## Setup in a dbt-synapse project

1. Install the adapter: `pip install dbt-synapse` (pulls in `dbt-sqlserver`).
2. Add a `profiles.yml` target of `type: synapse` with your workspace/SQL pool,
   database, schema and auth (e.g. `authentication: ActiveDirectoryServicePrincipal`).
3. Copy the files above into your project's `macros/`, `models/`, and `tests/`.
4. Add the exclude-list variable to `dbt_project.yml`:

```yaml
vars:
  scd2_hash_exclude_columns:
    - snapshot_date
    - account_id
    - modified_at
```

5. Build:

```bash
dbt seed
dbt run  --select +dim_account_scd2
dbt test --select dim_account_scd2 assert_scd2_one_current_per_account assert_scd2_no_overlapping_versions
```

## What changed from the DuckDB version, and why

| Concern | DuckDB | Synapse Dedicated SQL Pool |
|---------|--------|----------------------------|
| Hashing | `md5(...)` | `convert(varchar(64), hashbytes('SHA2_256', ...), 2)` |
| String concat | `a \|\| '\|' \|\| b` | `concat(a, '\|', b)` (also coerces types + NULL→'') |
| Boolean flag | `... as is_current` | `cast(case when ... then 1 else 0 end as bit)` |
| Date minus a day | `lead(...) - interval 1 day` | `dateadd(day, -1, lead(...))` |
| High date literal | `date '9999-12-31'` | `cast('9999-12-31' as date)` |
| Timestamp type | `timestamp` | `datetime2(0)` |
| Integer cast | `cast(x as integer)` | `cast(x as int)` |
| Inequality | `!=` | `<>` |
| Table layout | n/a | `dist='REPLICATE'`, `index='HEAP'` config |
| `is_current` in tests | `where is_current` | `where is_current = 1` |

### Graceful schema evolution

`dim_account_scd2` does not hardcode its attribute columns. At build time it
introspects `stg_d365_account_snapshots` and passes through whatever business
columns exist, so a new source column flows into the dimension automatically and
a removed column drops out instead of raising "invalid column name". Because the
model is a CTAS `table`, the physical dimension is rebuilt to match each run — no
`ALTER TABLE` needed.

The only required (contract) columns are `account_id`, `snapshot_date`, and
`attribute_hash`; losing one is a genuine break and fails loudly. Control columns
handled explicitly are listed in `scd2_passthrough_exclude` at the top of the
model.

### Distribution choice

`dim_account_scd2` is configured `dist='REPLICATE'` because dimensions are small
and replicating them avoids data movement when joined to large facts. For a big
fact table use `dist='HASH(<join_key>)'`; use `ROUND_ROBIN` only for staging/load.

### Scaling to hundreds of columns (the 254 wall)

dbt_utils' surrogate-key/hash macros cap out around **254 columns** on Synapse
because `CONCAT()` in a dedicated SQL pool accepts at most **254 arguments**.
`hash_all_except` avoids this entirely:

- It concatenates with the **`+` operator** (no argument-count limit) instead of
  `CONCAT()`.
- It **chunks** the columns (`chunk_size`, default 50), hashes each chunk to a
  64-char digest, then hashes the concatenated digests. This keeps every
  `HASHBYTES` input small and supports 400+ columns comfortably.

```jinja
-- default chunk size (50)
{{ hash_all_except(ref('my_wide_model'), var('scd2_hash_exclude_columns')) }}

-- tune chunk size for very wide or very narrow columns
{{ hash_all_except(ref('my_wide_model'), var('scd2_hash_exclude_columns'), chunk_size=30) }}
```

### HASHBYTES 8000-byte limit

In a dedicated SQL pool, `HASHBYTES` accepts at most 8000 bytes of input, and
chained `varchar(8000)` concatenation silently truncates to 8000 bytes. The
chunking above is what keeps each chunk's input under that limit — lower
`chunk_size` if individual columns are wide so `chunk_size × avg_width < 8000`.
The final combine step concatenates only `N × 65` chars (one 64-char digest per
chunk), which stays under 8000 until ~6000 columns.

## Making it adapter-agnostic (optional)

To keep one model that runs on both DuckDB and Synapse, convert the macro to
`adapter.dispatch` with `default__hash_all_except` (the `md5`/`||` version) and
`synapse__hash_all_except` (this file), and wrap the date math / boolean / high
date in small dispatched helper macros.
