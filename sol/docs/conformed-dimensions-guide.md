# Guide: Setting Up Conformed Dimensions

This guide explains how to use the `conform` macros to map messy, system-specific
source values onto a single canonical dimension, then land the dimension
surrogate key (`*_sk`) on fact tables.

The worked example is **country**:

| Source | Staging model | Raw country value | Fact |
|--------|---------------|-------------------|------|
| ABF | `stg_abf` | Alpha-2 code (`AU`, `US`) | `fct_abf_shipment` |
| ABS | `stg_abs` | Country name (`Australia`, `United States of America`) | `fct_abs_order` |

Both facts end up with the same `country_sk` from `dim_country_current`.

---

## Why this pattern exists

Without a shared conformance layer, each fact tends to invent its own join logic:

- string matching on every consumer
- duplicated `CASE` / mapping logic
- different spellings resolving to different keys
- expensive dimension scans repeated per model

The `conform` macros push that work into three cheap, reusable pieces:

1. **A mapping table** — source value → canonical business key
2. **A pre-materialized lookup** — hashed key → dimension attributes / SK
3. **Persisted conform keys on staging** — binary equality joins in facts

Design goals baked into the macros:

- no `run_query()` during compilation
- no generated `CASE` lists
- no per-consumer `ROW_NUMBER()`, dimension scan, or mapping scan
- one narrow lookup table, built once
- one equality join per distinct lookup/key pair (automatically reused)
- deterministic duplicate handling plus an auditable candidate count

---

## Mental model

```text
┌─────────────────┐     ┌──────────────────┐
│ Source systems  │     │ Canonical dim    │
│ ABF: alpha2     │     │ dim_country_     │
│ ABS: names      │     │   current        │
└────────┬────────┘     │  country_sk      │
         │              │  alpha2 (BK)     │
         ▼              └────────┬─────────┘
┌─────────────────┐              │
│ map_country     │──────────────┘
│ source_system   │  joins on canonical alpha2
│ source_value    │
│ canonical_value │
└────────┬────────┘
         ▼
┌─────────────────┐
│ lkp_country     │  conform_key → country_sk (+ attrs)
└────────┬────────┘
         ▲
         │ equality on country_conform_key
┌────────┴────────┐
│ stg_abf /       │  persist country_conform_key
│ stg_abs         │
└────────┬────────┘
         ▼
┌─────────────────┐
│ fct_*           │  keep country_sk only
└─────────────────┘
```

**Rule of thumb:** hashing and mapping happen when you build staging and
lookups. Downstream facts only do a binary equality join.

---

## Prerequisites

1. Copy `macros/conform.sql` and `macros/_conform_helpers.sql` into your dbt
   project's `macros/` folder (or depend on them as a package).
2. Have a **canonical dimension** with:
   - a surrogate key (`country_sk`)
   - a stable business key used for mapping joins (`alpha2`, `gender_code`, …)
   - any descriptive attributes consumers might need
3. Prefer **current / Type-1 style** dimension snapshots for the lookup join
   column (for example `dim_country_current`).

---

## Step-by-step setup

### Step 1 — Define the canonical dimension

Create one row per real-world entity. The business key (`alpha2` here) is what
mappings will target.

```sql
-- models/dimensions/dim_country_current.sql
select
    country_sk,
    alpha2,
    country_name
from {{ ref('raw_dim_country') }}
```

Example rows:

| country_sk | alpha2 | country_name |
|------------|--------|--------------|
| 1 | AU | Australia |
| 2 | US | United States |
| 3 | GB | United Kingdom |

Treat `country_sk` as the only key that facts should store.

---

### Step 2 — Build the source → canonical mapping

The mapping table translates each source system's native values onto the
dimension business key.

Required shape:

| Column | Purpose |
|--------|---------|
| `source_system` | Distinguishes identical strings from different systems |
| `source_value` | What the source actually sends |
| `canonical_value` | Matches the dimension join column (`alpha2`) |

```sql
-- models/dimensions/map_country.sql
select
    source_system,
    source_value,
    canonical_value
from {{ ref('raw_map_country') }}
```

Example rows from this demo:

| source_system | source_value | canonical_value |
|---------------|--------------|-----------------|
| ABF | AU | AU |
| ABF | US | US |
| ABS | Australia | AU |
| ABS | United States | US |
| ABS | United States of America | US |

**Mapping tips**

- Always include `source_system`. The same token (`1`, `M`, `AU`) can mean
  different things across systems.
- Map every known spelling / code you care about. Unmapped values become the
  unknown member (`country_sk = -1` by default).
- Do **not** add redundant rows that only differ by case or surrounding spaces.
  Keys are normalized with `upper(trim(...))`, so `AU` and `au` already collide
  to the same key. Duplicate normalized keys make
  `conform_lookup_unambiguous` fail.
- Prefer owning mappings in a governed table (seed, spreadsheet load, or MDM
  extract) rather than hard-coding them in SQL.

---

### Step 3 — Materialize the lookup once

The lookup is the join surface every fact uses. Build it with
`conform_lookup_config` + `conform_lookup`:

```sql
-- models/lookups/lkp_country.sql
{{ conform_lookup_config(size='small') }}

{{ conform_lookup(
    dim='dim_country_current',
    join_column='alpha2',
    output_columns=['country_sk', 'alpha2', 'country_name'],
    mapping='map_country',
    mapping_source_column='source_value',
    mapping_canonical_column='canonical_value',
    mapping_source_system_column='source_system',
    dedupe_order_by='d.country_sk'
) }}
```

What this produces:

| Column | Meaning |
|--------|---------|
| `conform_key` | SHA-256 of normalized `source_system` + `source_value` |
| `country_sk` | Canonical surrogate key |
| `alpha2`, `country_name` | Optional attributes pulled from the dimension |
| `conform_candidate_count` | How many dimension rows competed before dedupe |

**`conform_lookup` arguments**

| Argument | Required | Description |
|----------|----------|-------------|
| `dim` | yes | Dimension model name (passed to `ref()`) |
| `join_column` | yes | Dimension business key column |
| `output_columns` | yes | Dimension columns to expose on the lookup |
| `mapping` | no | Mapping model; omit only when source values already match the dimension key |
| `mapping_source_column` | no | Default `source_value` |
| `mapping_canonical_column` | no | Default `canonical_value` |
| `mapping_source_system_column` | no | Include when keys are system-scoped (recommended) |
| `dedupe_order_by` | no | `ORDER BY` for picking a winner; finish with a unique key |

**Sizing (`conform_lookup_config`)**

| `size` | Synapse behaviour | When to use |
|--------|-------------------|-------------|
| `small` | `DISTRIBUTION = REPLICATE`, clustered index on `conform_key` | Typical lookups (&lt; ~2 GB compressed) |
| `large` | `HASH(conform_key)` + clustered columnstore | Genuinely large lookups |

On DuckDB (this demo), both sizes simply materialize a table.

Prefer a full refresh / CTAS for lookups. Dimensions and mappings usually
change slowly, and replicated Synapse tables are expensive to maintain
incrementally.

---

### Step 4 — Persist conform keys in staging

Each source staging model hashes its raw country column **with the source
system encoded**. Materialize staging as a **table** so the binary key is
persisted.

**ABF (alpha-2 codes):**

```sql
-- models/staging/stg_abf.sql
select
    shipment_id,
    nullif(trim(alpha2_country), '') as alpha2_country,
    shipped_at,
    units,
    {{ conform_key('s.alpha2_country', source_system='ABF') }}
        as country_conform_key
from {{ ref('raw_abf_shipment') }} as s
```

**ABS (country names):**

```sql
-- models/staging/stg_abs.sql
select
    order_id,
    nullif(trim(country_name), '') as country_name,
    ordered_at,
    amount,
    {{ conform_key('s.country_name', source_system='ABS') }}
        as country_conform_key
from {{ ref('raw_abs_order') }} as s
```

**`conform_key` options**

| Call | Use when |
|------|----------|
| `conform_key('s.col', source_system='ABF')` | Source system is fixed for the model |
| `conform_key('s.col', source_system_column='s.source_system')` | Source system varies per row |
| `conform_key('s.col')` | No system scope (direct match to dimension key) |

Normalization applied before hashing:

1. cast / convert to string
2. trim whitespace
3. upper-case
4. treat blank as null
5. if a source system is present, concatenate `system + char(31) + value`
6. SHA-256 → fixed 32-byte key

Blank or null inputs yield a null `conform_key`, which later falls back to the
unknown member on the fact.

---

### Step 5 — Land the SK on fact tables

Facts should store the surrogate key, not the raw source label. Use
`conform_joins_ns()`, one or more `conform_join` / `conform` calls in the
select list, then render joins with `conform_joins()`.

**ABF shipments:**

```sql
-- models/marts/fct_abf_shipment.sql
{% set cj = conform_joins_ns() %}

select
    a.shipment_id,
    a.shipped_at,
    a.units,
    a.alpha2_country as source_country,  -- optional audit column
    {{ conform_join(cj, 'a.country_conform_key', 'lkp_country', 'country_sk') }}
        as country_sk
from {{ ref('stg_abf') }} as a
{{ conform_joins(cj) }}
```

**ABS orders:**

```sql
-- models/marts/fct_abs_order.sql
{% set cj = conform_joins_ns() %}

select
    a.order_id,
    a.ordered_at,
    a.amount,
    a.country_name as source_country,
    {{ conform_join(cj, 'a.country_conform_key', 'lkp_country', 'country_sk') }}
        as country_sk
from {{ ref('stg_abs') }} as a
{{ conform_joins(cj) }}
```

Both compile to a single left join on `conform_key`. Selecting multiple columns
from the same lookup/key pair reuses that join automatically:

```sql
{% set cj = conform_joins_ns() %}

select
    {{ conform_join(cj, 'a.country_conform_key', 'lkp_country', 'country_sk') }}
        as country_sk,
    {{ conform_join(cj, 'a.country_conform_key', 'lkp_country', 'country_name') }}
        as country_name
from {{ ref('stg_abf') }} as a
{{ conform_joins(cj) }}
```

**Defaults**

| Output column | Default when unmatched |
|---------------|------------------------|
| ends with `_sk` | `-1` |
| anything else | `null` |
| custom | pass `default="'UNKNOWN'"` (or a typed literal) |

Example:

```sql
{{ conform_join(
    cj,
    'a.country_conform_key',
    'lkp_country',
    'country_name',
    default="'UNKNOWN'"
) }}
```

**Required call order**

1. `{% set cj = conform_joins_ns() %}`
2. every `conform_join(...)` / `conform(...)` in the select list
3. `{{ conform_joins(cj) }}` once in the from/join section

Calling `conform_joins` twice for the same namespace, or joining after it has
already rendered, raises a compiler error.

---

### Step 6 — Add tests and build

Wire these tests on every lookup:

```yaml
# models/schema.yml
models:
  - name: lkp_country
    columns:
      - name: conform_key
        data_tests:
          - not_null
          - unique
      - name: country_sk
        data_tests:
          - not_null
    data_tests:
      - conform_lookup_unambiguous
```

| Test | Catches |
|------|---------|
| `not_null` / `unique` on `conform_key` | Broken key expression or mapping gaps that collapse keys |
| `conform_lookup_unambiguous` | Lookup had to pick among multiple candidates (`conform_candidate_count > 1`) |

If duplicates are intentional, set a business priority in `dedupe_order_by`
(always finish with a unique column) and relax the candidate-count test
explicitly.

Build with:

```bash
dbt build --select +lkp_country +fct_abf_shipment +fct_abs_order
```

Using `dbt build` (not only `dbt run`) ensures lookup tests fail before
downstream facts quietly absorb bad keys.

---

## End-to-end result

After building the country demo:

| Source | Raw value | `country_sk` | Canonical name |
|--------|-----------|--------------|----------------|
| ABF | `AU` | 1 | Australia |
| ABF | `au` | 1 | Australia (normalized) |
| ABS | `Australia` | 1 | Australia |
| ABS | `United States of America` | 2 | United States |
| ABF | `XX` | -1 | unmatched |
| ABS | `Atlantis` | -1 | unmatched |
| either | blank / null | -1 | unmatched |

ABF's alpha-2 codes and ABS's free-text names converge on the same SK grain.

Preview locally:

```bash
source .venv/bin/activate
export DBT_PROFILES_DIR=/home/deck/Projects/sol_macros

dbt show --select fct_abf_shipment
dbt show --select fct_abs_order
```

---

## Checklist for a new conformed dimension

Use this whenever you add another conformed attribute (currency, product, gender, …).

1. **Canonical dim** — `dim_<entity>_current` with `*_sk` + business key
2. **Mapping** — `map_<entity>` with `source_system`, `source_value`, `canonical_value`
3. **Lookup** — `lkp_<entity>` via `conform_lookup_config` + `conform_lookup`
4. **Tests** — `not_null` + `unique` on `conform_key`, plus `conform_lookup_unambiguous`
5. **Staging** — persist `{{ conform_key(...) }} as <entity>_conform_key` (table materialization)
6. **Facts** — `conform_join` to land `<entity>_sk` only; keep descriptive attrs on the dim
7. **Build** — `dbt build --select +lkp_<entity> +fct_...`

Suggested folder layout:

```text
models/
  dimensions/
    dim_country_current.sql
    map_country.sql
  lookups/
    lkp_country.sql
  staging/
    stg_abf.sql
    stg_abs.sql
  marts/
    fct_abf_shipment.sql
    fct_abs_order.sql
```

---

## Alternate path: normalize at join time

If you cannot persist a conform key on staging, set `normalize=true` on
`conform_join` / `conform` so the hash is computed in the fact SQL:

```sql
{% set cj = conform_joins_ns() %}

select
    e.employee_id,
    {{ conform(
        cj,
        'e.raw_gender',
        'lkp_gender',
        'gender_name',
        default="'UNKNOWN'",
        normalize=true,
        source_system_column='e.source_system'
    ) }} as gender_name
from {{ ref('stg_employee') }} as e
{{ conform_joins(cj) }}
```

Prefer the **persisted staging key** path in production:

- hashing runs once when staging is built
- fact joins stay narrow binary equality
- Synapse can HASH-distribute large facts and lookups on the same key

Use `normalize=true` for prototypes or one-off models only.

---

## Macro reference (consumer-facing)

| Macro | Role |
|-------|------|
| `conform_lookup_config(size=...)` | Materialization / distribution config for the lookup |
| `conform_lookup(...)` | Builds `conform_key` + dimension outputs |
| `conform_key(column, source_system=...)` | Expression for a persisted staging key |
| `conform_joins_ns()` | Creates the join namespace object |
| `conform_join(cj, column, lookup, output_column, ...)` | Selects an output; registers a shared join |
| `conform(...)` | Short alias for `conform_join` |
| `conform_joins(cj)` | Emits the accumulated `left join` clauses |
| `conform_lookup_unambiguous` | Test: no null keys and `candidate_count <= 1` |

`conform_join` / `conform` keyword arguments:

| Argument | Default | Notes |
|----------|---------|-------|
| `joins` | — | From `conform_joins_ns()` |
| `column` | — | Relation-qualified, e.g. `a.country_conform_key` |
| `lookup` | — | Lookup model name |
| `output_column` | — | Column to pull from the lookup |
| `default` | `_sk` → `-1`, else `null` | Unmatched fallback SQL expression |
| `alias` | `cj_N` | Optional join alias |
| `normalize` | `false` | Hash `column` at join time |
| `source_system` / `source_system_column` | none | Only valid with `normalize=true` |

---

## Operational guidance

### Refresh order

When mappings or the dimension change:

1. refresh `dim_*_current` and `map_*`
2. full-refresh `lkp_*`
3. rebuild staging if the key expression or source values changed
4. rebuild downstream facts (or let them pick up new SKs on next run)

Changing normalization logic (`_conform_normalized_value_expr` /
`_conform_key_expr`) requires a **controlled full refresh of every lookup and
staging model** that persists conform keys. Old and new hashes will not match.

### Unknown / unmatched members

Unmapped or blank source values resolve to:

- `*_sk = -1`
- descriptive fields `null` (or your `default`)

Keep a real unknown row in the dimension if BI tools require a joinable member,
and either map a dedicated source token to it or handle `-1` in reporting.

### What belongs on the fact

| Put on the fact | Keep on the dimension |
|-----------------|------------------------|
| `country_sk` | `alpha2`, `country_name`, region, etc. |
| optional raw `source_country` for audit | slowly changing attributes |

Facts stay narrow; conformed attributes are retrieved by joining the dimension
on `country_sk` when needed.

### Common failures

| Symptom | Likely cause |
|---------|--------------|
| `conform_lookup_unambiguous` fails | Two mapping rows normalize to the same key, or `dedupe_order_by` is not unique enough |
| Everything maps to `-1` | `source_system` string mismatch between staging `conform_key` and mapping rows |
| Compiler error: must be relation-qualified | Pass `a.country_conform_key`, not `country_conform_key` |
| Compiler error: namespace already rendered | `conform_joins(cj)` called before all `conform_join` select expressions |
| Case variants don't match | Expected — they should. If they don't, the lookup/staging key expressions drifted |

---

## Quick start with this demo project

```bash
cd /home/deck/Projects/sol_macros
source .venv/bin/activate
export DBT_PROFILES_DIR=/home/deck/Projects/sol_macros

dbt seed
dbt build --select +fct_abf_shipment +fct_abs_order

dbt show --inline "
select 'ABF' as src, source_country, country_sk from {{ ref('fct_abf_shipment') }}
union all
select 'ABS', source_country, country_sk from {{ ref('fct_abs_order') }}
order by 1, 2
"
```

You should see ABF codes and ABS names sharing the same `country_sk` values,
with unmatched rows at `-1`.
