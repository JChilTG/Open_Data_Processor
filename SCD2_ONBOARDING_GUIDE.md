# Onboarding a new SCD2 entity

Step-by-step guide to add a new dimension using the zero-copy SCD2 pattern
(history table → candidate view → published view → CTAS consumption table).

This guide uses **`dim_customer`** as the example. Replace names to match your
entity.

---

## 1. Prerequisites (once per project)

Confirm these exist before adding a second entity:

| Requirement | Notes |
|---|---|
| `audit` schema | Bootstrap creates tables inside it; schema must already exist |
| Macros in `macros/` | `scd2_zero_copy.sql`, `scd2_publish_table.sql`, `scd2_schema_drift.sql`, `scd2_rollback.sql`, `star_exclude.sql` |
| Hooks in `dbt_project.yml` | `on-run-start: scd2_bootstrap_audit()` and `on-run-end: scd2_approve_batches(results)` |

Hooks are **project-wide** — you do not add new hooks per entity. One
`scd2_approve_batches` call gates every model tagged `scd2_history`.

---

## 2. Decide the naming contract

Pick these four names and keep them consistent everywhere:

| Concept | Example | Rule |
|---|---|---|
| Consumption table / marker | `dim_customer` | `meta.scd2_model` value; also the final table name consumers query |
| History model | `dim_customer__history` | Must be tagged `scd2_history`; approval key = this model name |
| Candidate view | `dim_customer__candidate` | Tagged `scd2_candidate`; primary test target |
| Published view | `dim_customer__published` | Tagged `scd2_published`; source for CTAS refresh |
| Staging model | `stg_customer` | Must expose the contract columns below |
| Surrogate key column | `dim_customer_sk` | Pass into `history_meta_cols` if not `dim_entity_sk` |
| Natural key | `customer_id` | Used for `HASH(...)` dist and window partitions |
| Change date | `_landing_extract_date` | Becomes `valid_from` |
| Change hash | `attribute_hash` | Trusted from staging; never re-derived |

---

## 3. Staging contract

`stg_customer` must provide:

```text
customer_id              -- natural key (not null)
_landing_extract_date    -- change / extract date
attribute_hash           -- hash of tracked attributes
<all other attributes>   -- versioned automatically via star_exclude
```

Rules:

- **Do not** put `valid_from`, `valid_to`, or `is_current` in staging.
- When you add/remove an attribute, update the **upstream hash derivation**
  in the same change, or versions will not fire for that column.
- One row per `(customer_id, _landing_extract_date)` after staging dedupe
  is ideal; the history model also dedupes as a safety net.

---

## 4. Files to create

Copy the `dim_entity*` trio and rename. Suggested layout:

```text
models/scd2/
  dim_customer__history.sql
  dim_customer__candidate.sql
  dim_customer__published.sql
  scd2_dim_customer.yml

tests/   (optional but recommended)
  dim_customer__reconciles_to_latest_extract.sql
```

You do **not** create a dbt model named `dim_customer.sql`. The consumption
table is built by `scd2_refresh_published('dim_customer')` after approval.

---

## 5. What to change in each file

### 5.1 `dim_customer__history.sql`

| Find (entity template) | Replace with |
|---|---|
| `meta={'scd2_model': 'dim_entity'}` | `'dim_customer'` |
| `dist='HASH(entity_id)'` | `HASH(customer_id)` |
| `ref('stg_entity')` | `ref('stg_customer')` |
| `source_exclude = ['entity_id', ...]` | `['customer_id', '_landing_extract_date', 'attribute_hash']` |
| `entity_id` in SELECT / windows / SK | `customer_id` |
| `dim_entity_sk` | `dim_customer_sk` |
| SK concat uses `entity_id` | use `customer_id` |

Also pass the surrogate key name into the drift macro (default assumes
`dim_entity_sk`):

```jinja
{%- set attrs = scd2_resolve_attributes(
        source_relation  = ref('stg_customer'),
        history_relation = this,
        source_exclude   = ['customer_id', '_landing_extract_date', 'attribute_hash'],
        history_meta_cols = ['dim_customer_sk', 'customer_id', 'valid_from',
                             'attribute_hash', '_batch_id', '_loaded_at']
) -%}
```

Keep:

```jinja
tags=['scd2_history'],
pre_hook="{{ scd2_purge_unapproved(this) }}",
on_schema_change='ignore',
incremental_strategy='append',
```

### 5.2 `dim_customer__candidate.sql`

| Find | Replace |
|---|---|
| `meta={'scd2_model': 'dim_entity'}` | `'dim_customer'` |
| `ref('dim_entity__history')` | `ref('dim_customer__history')` |
| `partition by h.entity_id` | `partition by h.customer_id` |

Keep `tags=['scd2_candidate']`.

### 5.3 `dim_customer__published.sql`

| Find | Replace |
|---|---|
| `meta={'scd2_model': 'dim_entity'}` | `'dim_customer'` |
| `ref('dim_entity__history')` | `ref('dim_customer__history')` |
| `ab.model_name = 'dim_entity__history'` | `'dim_customer__history'` |
| `partition by a.entity_id` | `partition by a.customer_id` |

Keep `tags=['scd2_published']`.

**Critical:** `meta.scd2_model` must equal the consumption table name
(`dim_customer`). That is how `scd2_refresh_published` finds the view and
names the CTAS target.

### 5.4 `scd2_dim_customer.yml`

- Rename all three models.
- Rename `dim_entity_sk` → `dim_customer_sk` (unique + not_null on history).
- On candidate and published: `not_null` / single-current-row unique on
  `customer_id` (`where: "is_current = 1"`), `not_null` on `valid_from`.
- Keep `severity: error` and `store_failures: true` / `schema: audit` on
  gating tests so failed batches are blocked and inspectable.

### 5.5 Optional reconciliation test

Add a singular test that compares current rows in the candidate (or
published) view to the latest extract in `stg_customer`. Failures here also
block approval if severity is `error` and the test depends on a node with
`meta.scd2_model: dim_customer`.

---

## 6. Checklist before first run

- [ ] Staging model builds and has the contract columns
- [ ] All three models share `meta.scd2_model: 'dim_customer'`
- [ ] History tagged `scd2_history`; published tagged `scd2_published`
- [ ] Approval filter uses `model_name = 'dim_customer__history'`
- [ ] `history_meta_cols` includes your SK and natural key
- [ ] Dist / window partition column = natural key
- [ ] No dbt model named `dim_customer` (would fight the CTAS table)
- [ ] Project hooks already wired (section 1)

---

## 7. First run

```bash
dbt build --select dim_customer__history+
```

Expected happy path:

1. Audit table ensured
2. Purge no-op (empty / all approved)
3. Drift resolve (first run: no history table yet → create via incremental)
4. History appends changed rows with this `invocation_id`
5. Candidate + published views built
6. Tests run
7. Batch approved → `scd2_refresh_published('dim_customer')` creates
   `dim_customer` via CTAS + RENAME

Verify:

```sql
-- approval landed
select * from audit.scd2_approved_batches
where model_name = 'dim_customer__history';

-- consumers see the table
select top 100 * from <schema>.dim_customer where is_current = 1;
```

On failure: nothing is approved, `dim_customer` is unchanged (or still
absent on first ever fail), pending history rows remain until the next
run’s purge.

---

## 8. Day-2 operations

### Rollback (dry run first)

```bash
dbt run-operation scd2_rollback --args '{
  "model_name": "dim_customer__history",
  "to_datetime": "2026-07-01"
}'

dbt run-operation scd2_rollback --args '{
  "model_name": "dim_customer__history",
  "to_datetime": "2026-07-01",
  "dry_run": false
}'
```

This un-approves batches after the cutoff and refreshes `dim_customer`.
Add `"purge": true` only if you intend irreversible physical delete.

### Restore last rollback

```bash
dbt run-operation scd2_restore_last_rollback --args '{
  "model_name": "dim_customer__history"
}'
```

Only works if physical rows were not purged yet.

### Point consumers

Always read **`dim_customer`** (the table), never `__published` or
`__candidate`.

---

## 9. Common mistakes

| Mistake | Symptom |
|---|---|
| Wrong / missing `meta.scd2_model` | Batch approved but table never refreshes; or wrong table refreshed |
| Created `dim_customer.sql` as a view | Next build fights CTAS; consumers see a view again |
| Forgot `history_meta_cols` SK rename | Drift macro treats SK as an attribute column |
| `model_name` in EXISTS still says `dim_entity__history` | Published view always empty |
| Hash not updated when attrs change | Silent: no new versions for that attribute |
| Natural key not used in `HASH()` / `LEAD` partition | Shuffle / wrong SCD2 chaining |
| Tests severity `warn` | Failed tests do not block approval |

---

## 10. Minimal mental model

```text
stg_customer
    → dim_customer__history     (append-only facts)
        → dim_customer__candidate   (test the pending world)
        → dim_customer__published   (approved world only)
            → dim_customer          (CTAS table for consumers)
```

Publish = insert approval row + refresh table.  
Rollback = delete approval row(s) + refresh table.  
History rows are never updated.
