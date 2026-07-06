# Intermediate Layer & Conformed Dimensions

This is the layer where most of the team's judgment calls happen, and it's the layer
that keeps every domain speaking the same language. Read this one carefully.

## Purpose

Intermediate models sit between staging and marts and are where you:

1. Join staging models together within (or occasionally across) a domain.
2. Apply business logic that requires more than one row/entity (dedupe, surrogate key
   generation, business-rule filtering, light aggregation).
3. **Conform shared dimensions** so every domain that mentions "country", "currency",
   "product category", etc. means exactly the same thing.

Intermediate models are **not** exposed to BI tools. They're building blocks for marts.

## What "conformed dimension" means

A dimension is conformed when every fact table
that references it uses the same key, the same grain, and the same attribute values,
regardless of which source system fed the data. If `sales` says `country_iso3 = 'USA'`
and `support` says `country_iso3 = 'USA'`, an analyst can join `fct_orders` and
`fct_support_tickets` on `country_iso3` and get a correct answer. If one domain used
`'US'` and another used `'United States'`, that join silently breaks.

Every source system tends to represent shared concepts differently:

| Source | Raw value | | Source | Raw value |
|---|---|---|---|---|
| Salesforce | `United States` | | Shopify | `US` |
| NetSuite | `840` (numeric ISO code) | | Support desk | `usa` |

The job of the conformed dimension model is to be the **single place** this gets
resolved, so no one else has to think about it again.

## The pattern: crosswalk seed + conformed intermediate model

### 1. Maintain a master reference seed

Put the authoritative list of conformed values in `seeds/`, version-controlled like code:

```csv
# seeds/seed_country_codes.csv
country_iso2,country_iso3,country_name,region
US,USA,United States,Americas
GB,GBR,United Kingdom,Europe
DE,DEU,Germany,Europe
```

This is the single source of truth for what a "country" is in this warehouse. Nobody
edits this except through a reviewed PR.

### 2. Maintain a per-source crosswalk (when source values don't map cleanly)

If a source uses free text or non-standard codes, add a small crosswalk seed mapping its
raw values to the conformed key. Keep one crosswalk per source, not one giant table:

```csv
# seeds/seed_salesforce_country_crosswalk.csv
source_value,country_iso3
United States,USA
USA,USA
U.S.A.,USA
United Kingdom,GBR
```

### 3. Build one conformed dimension model per shared concept

This lives in `intermediate/conformed/` and is the only place that knows how to go from
"messy source value" to "conformed key":

```sql
-- intermediate/conformed/int_country__conformed.sql
{{ config(materialized='view') }}

with countries as (
    select * from {{ ref('seed_country_codes') }}
)

select
    country_iso2,
    country_iso3,
    country_name,
    region
from countries
```

And a per-source resolver that maps raw staging values onto it, e.g.:

```sql
-- intermediate/conformed/int_salesforce_country__resolved.sql
with stg as (
    select * from {{ ref('stg_salesforce__accounts') }}
),

crosswalk as (
    select * from {{ ref('seed_salesforce_country_crosswalk') }}
),

resolved as (
    select
        stg.salesforce_account_id,
        coalesce(crosswalk.country_iso3, 'UNKNOWN') as country_iso3
    from stg
    left join crosswalk
        on stg.country_raw = crosswalk.source_value
)

select * from resolved
```

Any domain model that needs a conformed country key joins to
`int_country__conformed` (for the attributes: name, region) via the resolved
`country_iso3` key produced above — it never re-derives the mapping itself.

### 4. Domain intermediate models consume the conformed output

```sql
-- intermediate/sales/int_customers__with_conformed_country.sql
with accounts as (
    select * from {{ ref('stg_salesforce__accounts') }}
),

country_resolved as (
    select * from {{ ref('int_salesforce_country__resolved') }}
)

select
    accounts.salesforce_account_id,
    accounts.account_name,
    country_resolved.country_iso3
from accounts
left join country_resolved
    using (salesforce_account_id)
```

## Rules for conforming a new dimension

- **Never conform inline inside a domain model.** If you find yourself writing a `case
  when raw_country in (...)` inside a sales-specific intermediate model, stop — that
  logic belongs in `intermediate/conformed/`, as a model any other domain can reuse.
- **One conformed model per concept**, not one per source. Sources get a small
  "resolver" model each; the conformed dimension itself (the seed-backed model) is
  singular.
- **Every conformed key must have an `UNKNOWN`/`N/A` fallback**, never a null that
  silently drops rows on an inner join downstream. Decide the fallback value once, per
  dimension, and document it.
- **New source, existing conformed dimension** → add a crosswalk seed and a resolver
  model for that source; do not touch the conformed dimension model itself unless the
  master list is genuinely missing a value.
- **New shared concept nobody has conformed yet** (e.g. "product category") → propose the
  seed + conformed model in a PR before building domain logic on top of it. This is a
  team decision, not a per-model one, because once two marts depend on incompatible
  definitions it's expensive to unwind.

## Common conformed dimensions to expect in most warehouses

Bootstrap these early since almost every domain touches them:

- `int_country__conformed` (ISO country codes)
- `int_currency__conformed` (ISO currency codes)
- `int_date__conformed` (calendar/fiscal date spine — generate with `dbt_utils.date_spine`
  rather than a seed)

## Other intermediate responsibilities (non-conforming)

- **Deduplication**: `qualify row_number() over (partition by ... order by ...) = 1`
  patterns live here, not in staging or marts.
- **Surrogate key generation**: use `{{ dbt_utils.generate_surrogate_key([...]) }}` for
  keys that will become dimension/fact keys in marts.
- **Business rule filters**: e.g. "only include orders that are not test accounts" — this
  is business logic, so it belongs here, not in staging.

## Checklist before opening a PR for an intermediate model

- [ ] If this model touches a dimension another domain also uses (country, currency,
      date, customer, product...) — does a conformed model for it already exist? If yes,
      `ref()` it. If no, is this really the first domain to need it, and should the
      conformed model be built first as its own PR?
- [ ] No raw/messy values leak through — every "conformed" column matches the master
      seed's values exactly.
- [ ] Grain of the model is documented in the schema.yml (one row per what?).
- [ ] Surrogate keys generated with `dbt_utils.generate_surrogate_key`, not
      hand-rolled string concatenation.
