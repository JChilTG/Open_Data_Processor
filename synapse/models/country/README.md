# Country mapping — usage guide

How to use the canonical country dimension, the override seed, and the source
bridges — and how to add a new source. Targets **Azure Synapse Dedicated SQL
Pool** (dbt-synapse).

## What's here

| File | Purpose |
|------|---------|
| `_country_sources.yml` | Declares the raw inputs: `market_table`, `source_as`, `source_af`. |
| `dim_country.sql` | Canonical dimension (`iso2`, `iso3`, `name`) from `market_table`, with canonical-name overrides applied. |
| `bridge_country_as.sql` | Crosswalk: Source AS **name** → canonical `iso3`. |
| `bridge_country_af.sql` | Crosswalk: Source AF **iso2** → canonical `iso3`. |
| `../../macros/country_crosswalk.sql` | Reusable macro that powers every bridge. |
| `../../seeds/country_overrides.csv` | Manual overrides (mapping + canonical name). |
| `_country_models.yml` | Tests (unique / not_null / relationships). |

### Lineage

```
market_table ─► dim_country ◄─ country_overrides (override_type = canonical_name)
                     ▲
country_overrides ───┤ (override_type = source_map)
(source_map rows)    │
source_as (name) ────┼─► bridge_country_as  (name -> canonical iso3)
source_af (iso2) ────┴─► bridge_country_af  (iso2 -> canonical iso3)
```

## How resolution works

For each distinct source value, `country_crosswalk` resolves in priority order:

1. **Override** — a matching `source_map` row in `country_overrides` wins.
2. **Automatic** — the normalized source value equals the canonical `name` / `iso2`
   / `iso3` in `dim_country`.
3. **Unmatched** — nothing matched: `canonical_iso3` is `NULL` and
   `match_type = 'unmatched'`.

Matching is case- and whitespace-insensitive, and every string comparison uses
`COLLATE DATABASE_DEFAULT` so a seed and a source with different collations don't
raise a collation-conflict error.

Each bridge outputs:

| Column | Meaning |
|--------|---------|
| `source_system` | e.g. `AS`, `AF` |
| `match_field` | `name` / `iso2` / `iso3` |
| `source_value` | the distinct value from the source |
| `canonical_iso3` | resolved canonical code (`NULL` if unmatched) |
| `canonical_iso2` | canonical iso2 for that code |
| `canonical_name` | canonical display name |
| `match_type` | `override` / `auto` / `unmatched` |

## Using the override seed

`country_overrides.csv` has one row per override and two modes via `override_type`.

### Mode 1 — map a source value to a canonical code (`source_map`)

Use when a source's value doesn't auto-match (different wording, non-standard
code, etc.).

| Column | Set to |
|--------|--------|
| `override_type` | `source_map` |
| `source_system` | the source label, e.g. `AS` |
| `match_field` | which field you're matching: `name`, `iso2`, or `iso3` |
| `source_value` | the exact source value (case/space-insensitive) |
| `canonical_iso3` | the canonical code to map it to |
| `canonical_name` | leave blank |

```csv
source_map,AS,name,South Korea,KOR,
source_map,AF,iso2,UK,GBR,
```

> AS calls it "South Korea" → map to `KOR`. AF uses `UK` for the United Kingdom →
> map to `GBR`.

### Mode 2 — override a canonical display name (`canonical_name`)

Use to correct or restyle the name that appears in `dim_country`.

| Column | Set to |
|--------|--------|
| `override_type` | `canonical_name` |
| `source_system` / `match_field` / `source_value` | leave blank |
| `canonical_iso3` | the canonical code |
| `canonical_name` | the display name to use |

```csv
canonical_name,,,,TUR,Turkey
canonical_name,,,,MMR,Myanmar (Burma)
```

### Rules

- `canonical_iso3` must exist in `market_table` (enforced by a relationships test).
- Avoid commas inside values, or quote the field (standard CSV rules).
- One `source_map` per `source_system` + `match_field` + `source_value`, and one
  `canonical_name` per code (enforced by singular tests in `../../tests/`).

## Consuming a bridge

Join your source rows back to the bridge to attach the canonical code. Normalize
both sides the same way the bridge does:

```sql
select
    s.*,
    b.canonical_iso3,
    b.canonical_name,
    b.match_type
from {{ source('country_raw', 'source_as') }} as s
left join {{ ref('bridge_country_as') }} as b
    on upper(ltrim(rtrim(s.name))) collate database_default
     = upper(ltrim(rtrim(b.source_value))) collate database_default
```

For AF, join `s.iso2` to `b.source_value` the same way.

## Finding what still needs an override

List unmatched values, then add `source_map` rows for them:

```sql
select source_system, source_value
from {{ ref('bridge_country_as') }}
where match_type = 'unmatched'
union all
select source_system, source_value
from {{ ref('bridge_country_af') }}
where match_type = 'unmatched'
order by source_system, source_value;
```

Workflow: run the bridges → review `unmatched` → add overrides to the seed →
`dbt seed && dbt run` → repeat until clean.

## Adding a new source

Say a new source **XX** provides a column `country_code` holding ISO3 codes.

1. **Register the table** in `_country_sources.yml`:

```yaml
      - name: source_xx
        description: Source XX. Provides an ISO3 country_code.
```

2. **Create the bridge** `bridge_country_xx.sql` — one macro call. Pick the
   `match_field` for the identifier the source provides (`name`, `iso2`, or
   `iso3`) and the column that holds it:

```sql
{{ config(materialized='table', dist='REPLICATE', index='HEAP') }}

{{ country_crosswalk(
    source_relation=source('country_raw', 'source_xx'),
    source_system='XX',
    match_field='iso3',
    source_key_column='country_code'
) }}
```

3. **Add tests** in `_country_models.yml`:

```yaml
  - name: bridge_country_xx
    columns:
      - name: source_value
        tests: [not_null]
      - name: canonical_iso3
        tests:
          - relationships:
              arguments:
                to: ref('dim_country')
                field: iso3
```

4. **Build and review**:

```bash
dbt run  --select bridge_country_xx
dbt test --select bridge_country_xx
```

5. **Add overrides** for any `unmatched` values (Mode 1, `source_system = 'XX'`,
   `match_field` matching step 2), then `dbt seed && dbt run --select bridge_country_xx`.

### Which `match_field` should I use?

| The source gives you… | `match_field` | Auto-matches against |
|-----------------------|---------------|----------------------|
| a country **name** | `name` | `dim_country.name` |
| a 2-letter code | `iso2` | `dim_country.iso2` |
| a 3-letter code | `iso3` | `dim_country.iso3` |

If a source provides several identifiers, prefer the most reliable (`iso3` >
`iso2` > `name`); you can still override individual mismatches.

## First-time setup

Point `_country_sources.yml` at your database/schema, then:

```bash
dbt seed --select country_overrides
dbt run  --select dim_country bridge_country_as bridge_country_af
dbt test --select dim_country bridge_country_as bridge_country_af
```
