# SQL Style Guide

Purely mechanical conventions — enforced so diffs are about logic, not formatting
arguments.

## Formatting

- Lowercase SQL keywords (`select`, `from`, `left join`, `where`), not `SELECT`/`FROM`.
- One column per line once a `select` has more than ~3 columns.
- Trailing commas.
- 4-space indentation, no tabs.
- Explicit `left join` / `inner join`, never bare `join`.
- Explicit `as` for all column and table aliases.

## CTE structure

Every non-trivial model follows import → logical → final:

```sql
with accounts as (                      -- import: one CTE per ref()/source()

    select * from {{ ref('stg_salesforce__accounts') }}

),

country_resolved as (

    select * from {{ ref('int_salesforce_country__resolved') }}

),

joined as (                             -- logical: the actual transformation

    select
        accounts.salesforce_account_id,
        accounts.account_name,
        country_resolved.country_iso3
    from accounts
    left join country_resolved
        using (salesforce_account_id)

)

select * from joined                    -- final: always a plain select from the last CTE
```

- One import CTE per `ref()`/`source()` call, named after the model it pulls from.
- Never `select *` from a `ref()`/`source()` inside a logical CTE — only in the import
  CTE. Every other CTE selects explicit columns.
- The final statement is always `select * from <last_cte>` — no extra logic bolted onto
  the final select.

## Jinja/macros

- Use `{{ dbt_utils.generate_surrogate_key([...]) }}` for all surrogate keys — never hand
  roll `concat`/`hash` logic per model.
- Config blocks (`{{ config(...) }}`) go at the very top of the file, before the first
  CTE.
- Don't inline complex Jinja logic inside a `select` — pull it into a macro under
  `macros/` if it's used in more than one model.

## What NOT to do

- No `select *` in any final model output — always explicit columns, so a source schema
  change doesn't silently change a downstream model's shape.
- No hardcoded environment-specific values (database names, schema names) — use `{{
  target.schema }}` / `{{ source() }}` / `{{ ref() }}` always.
- No commented-out code left in merged models.
