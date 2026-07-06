# Staging Layer

## Purpose

Staging models are a thin, 1:1 cleanup of a single `_src` bronze table. They exist so
that every downstream model works with clean, consistently typed, consistently named
columns — and never has to touch raw source quirks directly.

**One staging model per source table. No exceptions.**

## What a staging model IS allowed to do

- Rename columns to the team's naming convention (see
  [01-naming-conventions.md](01-naming-conventions.md)).
- Cast to correct/consistent data types (e.g. bronze `varchar` dates → `date`/`datetime2`).
- Trim whitespace, standardize case on free-text codes (`upper(trim(country_cd))`).
- Light, deterministic, row-level transformations (e.g. `case when` to normalize a
  boolean flag stored as `'Y'/'N'` into `true`/`false`).
- Drop columns nobody downstream needs (but don't be aggressive — cheap to keep, expensive
  to add back later).
- Filter out hard-deleted/test rows if the source marks them (e.g. `where is_deleted = 0`).

## What a staging model is NOT allowed to do

- **No joins.** A staging model reads from exactly one source table.
- **No business logic** that requires knowledge of other entities (that's intermediate).
- **No aggregation.**
- **No renaming into "conformed" business terms yet** — that happens in intermediate. A
  staging model standardizes source-level naming/typing only; it does not know about
  other domains' versions of the same concept.

## Structure

Every staging model follows this CTE shape:

```sql
-- stg_salesforce__accounts.sql
{{ config(materialized='view') }}

with source as (

    select * from {{ source('salesforce', 'account') }}

),

renamed as (

    select
        id                          as salesforce_account_id,
        name                        as account_name,
        upper(trim(billingcountry)) as country_raw,
        cast(createddate as datetime2) as created_at,
        isdeleted                   as is_deleted

    from source

)

select * from renamed
where is_deleted = 0
```

Notes:
- Keep the raw, un-conformed source value around under a `_raw` suffix
  (`country_raw`) when it will later be conformed in intermediate — this makes it
  possible to debug conforming failures by tracing back to what the source actually sent.
- Materialize staging models as **views** by default (see
  [05-synapse-dedicated-pool-guidance.md](05-synapse-dedicated-pool-guidance.md) for why —
  Synapse dedicated pools charge you in storage and load time for every physical table).

## Sources file

Every source system folder has a `_<source>__sources.yml` declaring the bronze tables it
reads from `_src`, with freshness checks where the loader supports a loaded-at column:

```yaml
version: 2

sources:
  - name: salesforce
    schema: _src
    tables:
      - name: account
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
      - name: opportunity
```

## Checklist before opening a PR for a staging model

- [ ] One source table in, one model out — no joins.
- [ ] All columns renamed to team convention, `snake_case`.
- [ ] Types cast explicitly, not left to implicit coercion.
- [ ] Declared in `_<source>__sources.yml` and documented in `_<source>__models.yml`.
- [ ] `not_null` + `unique` tests on the natural key.
