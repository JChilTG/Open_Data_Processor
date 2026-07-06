# Testing & Documentation Requirements

Because Synapse dedicated pools don't enforce constraints (see
[05-synapse-dedicated-pool-guidance.md](05-synapse-dedicated-pool-guidance.md)), dbt
tests are the *only* thing standing between a bad join and a wrong number on someone's
dashboard. Tests are not optional polish — they are the substitute for database-level
integrity.

## Minimum required tests, by layer

### Staging

- `not_null` and `unique` on the natural/source key.
- `accepted_values` on any enum-like/status column where the source has a known,
  bounded set of values.

```yaml
models:
  - name: stg_salesforce__accounts
    columns:
      - name: salesforce_account_id
        tests: [not_null, unique]
```

### Intermediate — conformed dimensions specifically

- The conformed model itself: `unique` + `not_null` on the conformed key
  (`country_iso3`).
- Every per-source resolver model: `not_null` on the conformed key **after** the
  `coalesce(..., 'UNKNOWN')` fallback (i.e. verify the fallback is actually catching
  everything — no true nulls should survive).
- A `relationships` test from each resolver back to the master conformed model, so a
  crosswalk typo that produces a code not in `seed_country_codes.csv` fails CI instead of
  silently producing an orphaned key downstream.

```yaml
models:
  - name: int_country__conformed
    columns:
      - name: country_iso3
        tests: [not_null, unique]

  - name: int_salesforce_country__resolved
    columns:
      - name: country_iso3
        tests:
          - not_null
          - relationships:
              to: ref('int_country__conformed')
              field: country_iso3
```

### Marts

- `unique` + `not_null` on every primary/surrogate key.
- `relationships` test from every foreign key in a `fct_` table to its `dim_`.
- Grain check: a `dbt_utils.unique_combination_of_columns` test if the grain is composite
  (e.g. `fct_orders` is one row per `order_id, line_number`).
- Row-count sanity where it matters (e.g. `dbt_utils.expression_is_true` guarding against
  negative amounts on an additive measure).

## Documentation requirements

Every model, at every layer, needs a `schema.yml` entry with:

- A one-line `description` stating what the model is and, for marts, its **grain**.
- A `description` on every conformed/foreign key column stating what it means and where
  it's conformed from — this is the field a new modeler greps for before reinventing a
  mapping that already exists.

```yaml
models:
  - name: fct_orders
    description: >
      One row per order line. Grain: order_id + line_number.
    columns:
      - name: country_key
        description: >
          Conformed country dimension key. Sourced via int_country__conformed;
          do not join on raw country text from any staging model.
```

Run `dbt docs generate` before opening a PR that adds a new model — a model with no
description in the generated docs site is treated the same as a missing test in review.

## CI expectations

- `dbt build` (run + test) must pass on every PR before merge.
- `dbt test --select tag:conformed_dimension` (tag your conformed dimension models and
  their resolvers with this tag) should be treated as a release gate — a break here means
  a join across domains will silently produce wrong numbers, which is worse than a build
  failure.
