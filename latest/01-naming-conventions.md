# Naming Conventions

Consistent names are what let a new modeler `grep` their way around the project. Follow
these exactly — don't invent variations.

## Folder structure

```
models/
  staging/
    <source_system>/
      _<source_system>__sources.yml
      _<source_system>__models.yml
      stg_<source_system>__<entity>.sql
  intermediate/
    conformed/
      int_country__conformed.sql
      int_currency__conformed.sql
      int_date__conformed.sql
    <domain>/
      int_<entity>__<transformation>.sql
  marts/
    <domain>/
      fct_<business_process>.sql
      dim_<entity>.sql
seeds/
  seed_country_codes.csv
  seed_currency_codes.csv
```

`<source_system>` = the system data comes from (`salesforce`, `netsuite`, `shopify`).
`<domain>` = the business domain the mart serves (`sales`, `finance`, `marketing`).

## Model naming

| Layer | Pattern | Example |
|---|---|---|
| Staging | `stg_<source>__<entity>.sql` (double underscore between source and entity) | `stg_salesforce__accounts.sql` |
| Intermediate (conformed dimension) | `int_<dimension>__conformed.sql` | `int_country__conformed.sql` |
| Intermediate (domain logic) | `int_<entity>__<what_it_does>.sql` | `int_orders__deduplicated.sql`, `int_customers__with_conformed_country.sql` |
| Fact mart | `fct_<business_process>.sql` (verb-free, process name, singular) | `fct_orders.sql`, `fct_invoice_payments.sql` |
| Dimension mart | `dim_<entity>.sql` (singular) | `dim_customer.sql`, `dim_country.sql` |

Never prefix a model with both `stg_` and a domain name, and never build a mart directly
off a `_src` table — it must pass through staging and (if it joins anything or touches a
conformed dimension) intermediate.

## Column naming

- `snake_case`, always.
- Primary/surrogate keys: `<entity>_key` for surrogate keys generated in dbt (e.g.
  `customer_key`), `<entity>_id` for the natural/source system id (e.g.
  `salesforce_account_id`). Never call a column just `id`.
- Foreign keys reference the entity they point to: `customer_key`, `country_key`.
- Conformed attributes carry the conformed name, not the source name — e.g. every model
  that has been through the country conforming step exposes `country_iso3`, not
  `country`, `country_cd`, `cntry`, or whatever the source called it.
- Booleans: `is_`/`has_` prefix (`is_active`, `has_discount`).
- Dates/timestamps: suffix with `_date` or `_at` (`order_date`, `created_at`), always UTC
  unless documented otherwise.
- Amounts: suffix with the unit or currency context where ambiguity is possible
  (`amount_usd`, `amount_local_ccy`).

## Source and ref hygiene

- `{{ source() }}` is only ever called inside `staging/` models.
- Every other layer uses `{{ ref() }}` exclusively. If you're tempted to `source()` from
  inside an intermediate or mart model, that's a sign a staging model is missing.

## Database schema naming (dev vs prod)

Each layer gets its own database schema, set via a `+schema` config per folder in
`dbt_project.yml` (`staging`, `intermediate`, `marts`, or per-domain schemas like
`marts_finance` if a domain's marts should be isolated) — never left to dbt's default of
lumping everything into the target's base schema.

A custom `generate_schema_name` macro (`macros/generate_schema_name.sql`) controls how
that custom schema combines with the environment:

- **In prod** (the target named in the `prod_target_name` var, default `'prod'`): the
  schema is exactly the configured name — `intermediate`, `marts`, `marts_finance`.
  Clean, predictable names for BI tools and anyone browsing the warehouse.
- **Everywhere else** (a developer's own dev target, CI, ...): the schema is prefixed
  with that target's own schema — e.g. `dbt_jdoe_marts_finance` — so every developer's
  runs land in their own sandbox and never collide with prod or each other.

This means a model with no `+schema` config always just builds in the target's base
schema regardless of environment — only layers/domains that have opted into a custom
schema get this dev/prod routing. If your profile's production target isn't literally
named `prod`, set `vars.prod_target_name` in `dbt_project.yml` accordingly (see the
comment there) — do not edit the macro itself for this.
