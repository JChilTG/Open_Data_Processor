# Marts Layer

## Purpose

Marts are the consumption layer: star-schema `fct_`/`dim_` tables that BI tools and
analysts query directly. Everything upstream exists to make this layer simple, fast, and
correct.

## Organize by business domain, not by source system

```
marts/
  sales/
    fct_orders.sql
    dim_customer.sql
  finance/
    fct_invoice_payments.sql
  shared/
    dim_country.sql
    dim_date.sql
```

A `marts/shared/` (or `marts/core/`) folder holds dimensions that are genuinely reused
across multiple domains — `dim_country`, `dim_date`, `dim_currency` — built directly on
top of the corresponding `intermediate/conformed/` model. **Every domain's fact table
joins to the same `dim_country`; nobody builds their own `sales.dim_country`.** This is
the payoff of the conforming work done in the intermediate layer — see
[03-intermediate-layer-and-conformed-dimensions.md](03-intermediate-layer-and-conformed-dimensions.md).

## Fact tables (`fct_`)

- Name after the business process, not the source table: `fct_orders`, not
  `fct_salesforce_opportunities`.
- **Declare the grain explicitly** in the model's schema.yml description — "one row per
  order line" vs "one row per order" is not obvious from the SQL and must not be left to
  guesswork.
- Contain foreign keys to dimensions (`customer_key`, `country_key`, `date_key`) and
  numeric measures. Avoid descriptive/text attributes in facts — those belong on
  dimensions.
- Additive measures should be genuinely additive across every dimension in the table. If
  a measure isn't additive across some dimension (e.g. a balance snapshot), document it
  and consider a semi-additive/snapshot pattern instead.
- Build from `intermediate/` models only — never straight from `staging/`.

## Dimension tables (`dim_`)

- Singular business entity, one row per natural key (or per SCD-tracked version, if
  you're implementing slowly changing dimensions — document which type: SCD1 overwrite
  vs SCD2 history).
- Carry the surrogate key (`customer_key`) generated in intermediate, plus the natural
  key for traceability (`salesforce_account_id`).
- Shared dimensions (`dim_country`, `dim_date`, `dim_currency`) live in `marts/shared/`
  and are built directly from their `intermediate/conformed/` counterpart — thin pass
  through, no new logic:

```sql
-- marts/shared/dim_country.sql
{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['country_iso3']) }} as country_key,
    country_iso3,
    country_iso2,
    country_name,
    region
from {{ ref('int_country__conformed') }}
```

## Checklist before opening a PR for a mart model

- [ ] Grain is stated in plain English in the schema.yml description.
- [ ] All dimension foreign keys resolve to a `dim_` model that exists (no dangling
      keys) — verified with a `relationships` test.
- [ ] Shared dimensions (country/currency/date/etc.) are `ref()`'d from `marts/shared/`,
      not rebuilt locally.
- [ ] Materialized as a physical `table` (see
      [05-synapse-dedicated-pool-guidance.md](05-synapse-dedicated-pool-guidance.md) for
      distribution/index config) — marts are never views.
- [ ] Primary key uniqueness test passes.
