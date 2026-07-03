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
  macros/country_crosswalk.sql            resolve a source value to canonical ISO3
  seeds/country_overrides.csv             manual mapping + canonical-name overrides
  models/country/                         dim_country + AS/AF bridges + sources/tests
  models/staging/stg_..._core.sql         typed/cleaned columns (view)
  models/staging/stg_..._snapshots.sql    adds attribute_hash via the macro (view)
  models/marts/dim_account_scd1.sql            SCD1 built from raw snapshots
  models/marts/dim_account_scd1_from_scd2.sql  SCD1 derived from the SCD2 table (template)
  models/marts/dim_account_scd2.sql            SCD2 dimension (concrete example)
  models/marts/dim_scd2_template.sql           SCD2 template (full daily snapshots)
  models/marts/dim_scd2_delta_template.sql     SCD2 template (CDC / delta append)
  tests/                                       singular tests, adapted for BIT/T-SQL
```

### Snapshot vs delta SCD2 templates

Pick the template that matches how your source lands:

| | `dim_scd2_template` | `dim_scd2_delta_template` |
|---|---|---|
| Source | full daily snapshot (every key, every day) | append of only changed rows (CDC) |
| Version boundary | hash differs from previous day | every appended delta row |
| Deletes | inferred from absence in latest snapshot | explicit delete/operation flag |
| Grain | day | `day` or `timestamp` |
| No-op handling | inherent (hash comparison) | optional hash collapse |

The delta template supports both day and timestamp effective-dating, an optional
`delete_flag_column` (a delete delta becomes the latest version with
`is_deleted = 1`), an optional `hash_column` to collapse unchanged deltas, and an
optional `tiebreak_column` to dedupe multiple rows per key+date. Configure it via
its "Template configuration" block; a deleted key's active state is
`is_current = 1 and is_deleted = 0`.

### Reusable SCD2 template

`dim_scd2_template.sql` is the generic form of `dim_account_scd2`. To build an
SCD2 for a new entity, copy it, rename the file, and edit only the "Template
configuration" block:

```jinja
{%- set source_relation = ref('stg_d365_account_snapshots') -%}
{%- set natural_key = 'account_id' -%}
{%- set snapshot_date_column = 'snapshot_date' -%}
{%- set hash_column = 'attribute_hash' -%}
{%- set surrogate_key_name = 'account_sk' -%}
{%- set high_date = "cast('9999-12-31' as date)" -%}
```

Every CTE (partition, joins, effective-dating, surrogate key) is driven by those
values, and business columns are discovered at build time, so the template
survives source schema changes.

### Two ways to get SCD1

- `dim_account_scd1` rebuilds current-state directly from the daily snapshots.
- `dim_account_scd1_from_scd2` **collapses the SCD2 table** by keeping
  `is_current = 1` and dropping the SCD2 framework columns. It's cheaper and
  guaranteed consistent with the SCD2 history. Deleted accounts have
  `is_current = 0` in SCD2, so they are naturally excluded. It is written as a
  reusable template — change the four values in its "Template configuration"
  block (source relation, natural key, surrogate key name, framework columns) to
  reuse it for any SCD2 dimension.

## SCD1 vs SCD2

Both build from the same daily snapshots and share the Synapse conventions and
graceful schema-evolution approach.

| | `dim_account_scd1` | `dim_account_scd2` |
|---|---|---|
| Grain | one row per account (latest) | one row per account *per version* |
| History | overwrite, none kept | full, via `effective_from/to_date` |
| Key | `account_sk` = hash(account_id) | `account_sk` = hash(account_id + effective_from_date) |
| Deletes | `is_deleted` BIT flag (soft) | version closed on last-seen date |
| Current row | every row (filter `is_deleted = 0`) | `is_current = 1` |

SCD1 selects the latest snapshot per key with `ROW_NUMBER() ... WHERE _rn = 1`
(Synapse has no `QUALIFY`). For a current-and-active-only table, filter
`where is_deleted = 0`, or hard-delete by restricting to accounts present in the
latest extract.

## Country mapping (canonical + source crosswalks)

Maps disparate country sources onto a canonical list (`market_table`: iso2, iso3,
name) with a user-controlled override seed.

```
market_table ─► dim_country ◄─ country_overrides (canonical_name overrides)
                    ▲
country_overrides (source_map) ─┐
source_as (name) ───────────────┼─► bridge_country_as  (name  -> canonical iso3)
source_af (iso2) ───────────────┴─► bridge_country_af  (iso2  -> canonical iso3)
```

- `dim_country` applies `canonical_name` overrides on top of `market_table`.
- `bridge_country_as` / `bridge_country_af` call the `country_crosswalk` macro to
  resolve each source value **override first, then automatic match**, leaving
  `canonical_iso3` NULL (`match_type = 'unmatched'`) when nothing matches so gaps
  are visible.
- Matching is case/whitespace-insensitive and uses `COLLATE DATABASE_DEFAULT`
  everywhere to avoid seed-vs-source collation conflicts.

### The override seed (`country_overrides.csv`)

One seed, two modes via `override_type`:

| override_type | Purpose | Columns used |
|---------------|---------|--------------|
| `source_map` | map a source value to a canonical code | `source_system`, `match_field` (`name`/`iso2`/`iso3`), `source_value`, `canonical_iso3` |
| `canonical_name` | override a canonical code's display name | `canonical_iso3`, `canonical_name` |

```csv
override_type,source_system,match_field,source_value,canonical_iso3,canonical_name
source_map,AS,name,South Korea,KOR,
source_map,AF,iso2,UK,GBR,
canonical_name,,,,TUR,Turkey
```

### Reuse for another source

Add a source to `_country_sources.yml`, then create a one-line bridge:

```sql
{{ country_crosswalk(source('country_raw','source_xx'), 'XX', 'iso3', 'country_code') }}
```

Point `_country_sources.yml` at your database/schema before running:
`dbt seed && dbt run --select dim_country bridge_country_as bridge_country_af`.

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
