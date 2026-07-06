# Synapse Dedicated SQL Pool Guidance

Dedicated SQL pools have MPP-specific behavior that generic dbt advice doesn't cover.
These rules apply on top of everything else in this guide.

## Materialization by layer

| Layer | Materialization | Why |
|---|---|---|
| Staging | `view` | No storage cost, always reflects `_src` live, and Synapse dedicated pools bill/queue for every physical table build — don't pay that cost for a pure passthrough. |
| Intermediate | `view` by default; `table` only if a downstream model reads it repeatedly and the join/aggregation is expensive enough that recomputing it each time is a measurable cost | Views keep the DAG cheap to rebuild; promote to `table` deliberately, not by default. |
| Marts | `table` (CTAS) | Marts are read by BI tools repeatedly and need CCI performance; views over multiple joined views get slow on dedicated pools. |
| Conformed dimension resolvers (`intermediate/conformed/`) | `view`, except very large per-source crosswalks joined frequently — then `table` | Small in row count; the seed + crosswalk data is tiny. |

Never use `ephemeral` materialization for anything non-trivial on Synapse — ephemeral
models inline as CTEs, and dedicated pools' query optimizer handles deeply nested CTEs
worse than materialized views/tables. Keep ephemeral for genuinely tiny, single-use
renames only.

## Distribution (`dist`)

Set explicitly on every physical table via model config — do not leave it to the
adapter default:

```sql
{{ config(
    materialized='table',
    dist='HASH(customer_key)',
    as_columnstore=true
) }}
```

Rules of thumb:

- **Large fact tables** → `HASH(<column>)` on the column most commonly used in joins or
  `GROUP BY` (usually the highest-cardinality foreign key, e.g. `customer_key`). Avoid
  hashing on a low-cardinality or skewed column (e.g. `country_key` if 80% of rows are
  one country) — that creates data skew across distributions and kills parallelism.
- **Small dimension tables** (roughly under a few GB / a few million rows — `dim_country`,
  `dim_currency`, `dim_date`) → `REPLICATE`. A full copy on every compute node means joins
  never require data movement.
- **Large, frequently-joined dimensions** (e.g. `dim_customer` with millions of rows) →
  `HASH` on the dimension's own key, matching the hash key used on the fact tables that
  join to it wherever practical, to avoid shuffle at query time.
- **Staging views** → distribution doesn't apply (views aren't materialized storage).
- When in doubt and no clear hash candidate exists, `ROUND_ROBIN` is a safe, if
  unoptimized, fallback — better than a skewed hash key.

## Indexing

```sql
{{ config(
    materialized='table',
    dist='HASH(customer_key)',
    as_columnstore=true
) }}
```

- **Default for facts and any dimension over ~60M rows or so**: Clustered Columnstore
  Index (CCI) — set `as_columnstore=true` (or your adapter's equivalent index config).
  This is the standard for analytical workloads on dedicated pools.
- **Small dimension tables** and any staging/transient table that's mostly written once
  and read via point lookups: `HEAP` is fine and avoids CCI compression overhead on tiny
  tables.
- Check the exact config keys (`dist`, `index`, `as_columnstore`) against the specific
  version of the Synapse dbt adapter the project pins — these have changed across adapter
  versions, so treat the syntax above as the pattern, and confirm against
  `dbt_project.yml`/adapter docs before copy-pasting into a PR.

## Constraints and testing

Synapse dedicated pools do not enforce primary key, foreign key, or unique constraints
(`NOT ENFORCED` only, used for optimizer hints at best). This means:

- **Referential integrity must be enforced through dbt tests, not the database.** Every
  mart's foreign keys get a `relationships` test against the referenced `dim_`; every
  primary/surrogate key gets `unique` + `not_null`.
- Do not skip these tests thinking "the database has a PK/FK anyway" — it doesn't
  meaningfully enforce them here.

## T-SQL surface area gotchas

Dedicated SQL pool supports a large subset of T-SQL, but it's a subset — code that's
fine on SQL Server/Azure SQL DB can fail here. Confirmed against Microsoft's current
[Synapse SQL feature comparison](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/overview-features):

- **No `OFFSET`/`FETCH`** in a `SELECT`. Use `ROW_NUMBER() OVER (...)` in a CTE and
  filter on the rank instead — this is also how dedup/precedence logic should always be
  written here (see [08-conformed-dimension-framework.md](08-conformed-dimension-framework.md)
  for a worked example).
- **No `QUALIFY`** — that's Snowflake/BigQuery syntax, not T-SQL at all. The
  `ROW_NUMBER()` + outer `WHERE` pattern above is the substitute.
- Window/analytic functions (`ROW_NUMBER`, `RANK`, `LAG`/`LEAD`, etc.) **are** fully
  supported — don't avoid them.
- All string functions are supported **except `STRING_ESCAPE` and `TRANSLATE`**. `TRIM`
  is fine.
- **Collation conflicts** are the most common real-world surprise: joining a
  bronze-sourced text column against a table with a different collation (e.g. a dbt
  seed, which gets the database default) raises "Cannot resolve the collation
  conflict." Fix this at the column level — cast/normalize once where the value first
  enters the model, not with ad-hoc `COLLATE` sprinkled into large repeated joins
  (expensive on big distributed tables per Microsoft's own guidance). See
  `normalize_raw_value()` in
  [08-conformed-dimension-framework.md](08-conformed-dimension-framework.md) for the
  pattern this project uses.
- Scalar user-defined functions, table variables, and cursors are **not** supported.

## Incremental models

- Dedicated pools don't support `MERGE` the same way transactional engines do; confirm
  which `incremental_strategy` the pinned Synapse adapter supports (commonly
  `delete+insert` or `append`) before assuming `merge` works.
- Only use `incremental` materialization for genuinely large fact tables where full
  rebuild is measurably too slow — don't default to it. Full-refresh `table` is simpler
  to reason about and should be the default until proven too slow.

## Statistics

Dedicated pools do not always auto-create statistics as aggressively as expected for
CTAS output. Rather than every modeler remembering to hand-write a `post_hook` on every
new mart, use the shared `update_statistics()` macro (`macros/update_statistics.sql`)
and wire it as a **project-level default** so it's automatic:

```yaml
# dbt_project.yml
models:
  your_project:
    marts:
      +materialized: table
      +post-hook: "{{ update_statistics() }}"
```

That runs a whole-table `UPDATE STATISTICS` after every mart rebuild with zero
per-model effort. For a large fact table where a full-table scan is too expensive, or
where only the join/filter columns actually matter to the optimizer, override with a
targeted, model-level `post_hook` instead:

```sql
{{ config(
    materialized='table',
    dist='HASH(customer_key)',
    as_columnstore=true,
    post_hook=update_statistics(columns=['customer_key', 'order_date'], sample_percent=25)
) }}
```

See `macros/update_statistics.sql` for the full parameter list (`columns`, `fullscan`,
`sample_percent`) and a note on how project-level and model-level hooks combine (they
run in addition to each other, not instead).

## Summary defaults (copy-paste starting point)

```yaml
# dbt_project.yml
models:
  your_project:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table
      +as_columnstore: true
```

Override `dist` per-model — there is no safe project-wide default for distribution since
it depends on each table's join pattern and cardinality.
