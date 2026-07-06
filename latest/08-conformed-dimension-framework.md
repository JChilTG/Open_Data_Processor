# Conformed Dimension Framework (Reference Implementation)

[03-intermediate-layer-and-conformed-dimensions.md](03-intermediate-layer-and-conformed-dimensions.md)
describes the *pattern*. This document describes the actual, runnable framework in this
repo that implements it generically — built once for country, but designed so adding
currency, product category, or any other shared dimension later means adding data, not
code.

This repo is also a small working dbt project (`dbt_project.yml` + `seeds/` + `macros/` +
`models/`) implementing the pattern. The SQL is written specifically for Synapse
dedicated SQL pool (not generic/adapter-agnostic — this team only targets Synapse, so
the macros assume it rather than branching on adapter type), so `dbt seed && dbt run &&
dbt test` needs a Synapse profile pointed at a dev/UAT pool to actually execute; see
"Synapse dedicated SQL pool reliability" below for what was verified and how.

## Why a framework instead of one-off per-dimension SQL

The naive version of conforming — a `case when` per source, per dimension — doesn't
scale: every new source needs its own bespoke SQL, logic drifts between dimensions, and
nobody can tell where a mapping decision was made without reading the SQL top to bottom.
This framework separates the three things that actually vary from the one thing that
never does:

| Varies per dimension/source | Never varies |
|---|---|
| what the canonical values are (seeds) | how a raw value gets resolved to a canonical key (the macro) |
| what aliases/overrides exist (seeds) | |

Add a dimension or a source by adding *data* (seeds + one config entry), not by writing
new resolution logic.

## The four files that make up one conformed dimension

For country, these already exist in `seeds/conformed_dimensions/country/`:

| File | Purpose | Who maintains it |
|---|---|---|
| `seed_country_codes.csv` | Canonical master list — the definition of "country" in this warehouse | Reviewed PR only, rarely changes |
| `seed_country_aliases.csv` | Non-obvious raw values that don't match the master list's own columns: ISO numeric codes, historical names, common informal spellings (`"U.K."`, `"840"`, `"Holland"`) | Any modeler, whenever a new source reveals a new alias worth generalizing |
| `seed_country_overrides.csv` | Manual, human-curated corrections — the escape hatch for one specific source's specific bad data, or a business decision that overrides the "correct" mapping | Any modeler; every row requires a `reason` |
| `vars.conformed_dimensions.country` entry in `dbt_project.yml` | Wires the three seeds above together and names the canonical key column + which columns count as a "direct match" | Whoever bootstraps the dimension |

Plus shared logic, used by every dimension:

- `macros/conformed_dimensions/conform_dimension_mapping.sql` — given a dimension name
  and (optionally) a source system, returns a `raw_value -> canonical_key` lookup.
- `macros/conformed_dimensions/dimension_unknown_value.sql` — returns the dimension's
  configured fallback key as a SQL literal, so every resolver model coalesces
  consistently instead of hardcoding `'UNKNOWN'` in ten different places.
- `macros/conformed_dimensions/normalize_raw_value.sql` — the one, shared definition of
  "how do we make a raw value comparable": cast, trim, uppercase, and pin collation to
  the database default. Use this on **both** sides of any join to the mapping — inside
  the macro and in every resolver model — so the join key never silently diverges. See
  "Synapse dedicated SQL pool reliability" below.

## Resolution precedence

When a raw value could match more than one input, this is the order that wins (highest
first) — implemented as a `priority` column + `row_number()` in the macro:

1. **Source-specific override** — this exact source has a documented reason to map this
   value differently than anyone else would.
2. **Global override** — a documented, cross-source correction (e.g. every source that
   sends the literal string `"UNMAPPED"` should resolve to the Unknown member).
3. **Alias** — a known, general-purpose non-obvious mapping (ISO numeric code, informal
   name).
4. **Direct match** — the raw value already equals one of the canonical seed's own
   columns (iso2, iso3, or name), case/whitespace-insensitive. This is what makes "some
   sources send 2-char, some 3-char, some the full name" work with zero extra
   configuration — no alias needed for a source that just sends a valid code or name.

A raw value that matches **nothing** (typo, garbage, a genuinely new country nobody's
added yet) produces no row in the mapping at all. The calling resolver model is
responsible for `coalesce(...)`-ing that to the dimension's `unknown_value` — see the
worked example below. This is deliberate: it means "unmatched" and "matched to Unknown"
are both visible and testable, rather than the macro silently swallowing bad data.

## Synapse dedicated SQL pool reliability

This project targets Synapse dedicated SQL pool only, so the macros are written
directly against its T-SQL surface area rather than hedging for other engines — no
adapter-detection branching, just the syntax dedicated pools actually support:

- **Precedence uses `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY priority)`, not
  `OFFSET/FETCH` and not `QUALIFY`.** Dedicated SQL pools do not support `OFFSET/FETCH`
  in a `SELECT`, and `QUALIFY` isn't T-SQL at all (it's Snowflake/BigQuery). Window
  functions like `ROW_NUMBER` are fully supported on dedicated pools, so that's what the
  "highest precedence wins" logic is built on.
- **`TRIM()` is used directly**, not `LTRIM(RTRIM(...))`. Dedicated SQL pools support all
  T-SQL string functions except `STRING_ESCAPE` and `TRANSLATE` — `TRIM` is fine.
- **Collation is always normalized to `COLLATE DATABASE_DEFAULT`.** This is the one
  failure mode most likely to bite in production and least likely to show up in local
  testing: a resolver model joins a bronze/staging column (whatever collation the
  source loader gave it — PolyBase/COPY INTO often preserve or default to something
  other than the database's collation) against dbt seed data (which gets the database's
  default collation). Compare those directly and dedicated SQL pool raises "Cannot
  resolve the collation conflict." Microsoft's own guidance is to fix this at the column
  level rather than add `COLLATE` inside large, repeated joins, since that can be
  expensive on big hash/round-robin distributed tables. `normalize_raw_value()` applies
  `COLLATE DATABASE_DEFAULT` at the same point it already casts/trims/uppercases every
  value — a cost that's already being paid per row, not a new expensive operation.

**The practical implication for anyone writing a resolver model:** always build your
`raw_value` using `{{ normalize_raw_value('your_column') }}`, never a hand-rolled
`upper(trim(...))`. If you ever do hit a collation conflict error that this doesn't
cover (e.g. two bronze sources joined to each other before conforming, upstream of this
framework), that's a sign the fix belongs on the staging model's column definition, not
as a one-off `COLLATE` sprinkled into a downstream join — see
[02-staging-layer.md](02-staging-layer.md).

**How this was verified without a live Synapse pool:** there's no dedicated SQL pool
available in this environment to run against, and dbt's own Synapse adapter needs one
(no local emulator exists). The macro's SQL was checked two ways instead: (1) every
function/clause it uses (`ROW_NUMBER`, `TRIM`, `COLLATE DATABASE_DEFAULT`, `CAST`,
`UNION ALL`) was cross-checked against Microsoft's current
[Synapse SQL feature comparison](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/overview-features)
docs rather than assumed; (2) the macro was rendered standalone (outside of a real dbt
run, with `ref`/`var` stubbed) to inspect the exact generated SQL text for correctness —
this is how a whitespace bug that silently dropped a required space before `collate`
was caught before it ever reached a real warehouse. Before this framework goes live,
run it once against a real dev/UAT Synapse pool as a final check — the static
verification here is a strong signal, not a substitute.

## The `UNKNOWN` row lives in the canonical seed itself

`seed_country_codes.csv` includes a real row: `ZZ, UNKNOWN, Unknown or Not Provided,
Unknown`. This matters for referential integrity — a fact table's `country_key` foreign
key can always join to a real `dim_country` row, even for unresolvable source data,
instead of producing a null that breaks a `relationships` test or an inner join in a BI
tool. Every conformed dimension you add should follow this same convention: the fallback
sentinel is a real, documented member of the dimension, not a null.

## Worked example: `demo_source`

`seeds/conformed_dimensions/country/demo/seed_demo_source_country_raw.csv` fakes a
staging table with deliberately messy input, and
`models/intermediate/conformed/country/examples/int_demo_source_country__resolved.sql`
resolves it — run `dbt seed && dbt run --select int_demo_source_country__resolved` and
inspect the output. It exercises every precedence level:

| `country_raw` | Resolves via | Result |
|---|---|---|
| `US`, `USA`, `United States` | direct match (iso2 / iso3 / name) | `USA` |
| `840`, `U.K.`, `Great Britain`, `Deutschland` | alias | `USA`* / `GBR` / `GBR` / `DEU` |
| `LEGACY-FR` | source-specific override (`demo_source` only) | `FRA` |
| `UNMAPPED` | global override | `UNKNOWN` |
| `Wakanda` | no match anywhere → coalesced fallback | `UNKNOWN` |

\* `840` is the ISO 3166-1 numeric code for the United States.

## How to: onboard a new source for an existing dimension (e.g. country)

1. Build the normal `stg_<source>__<entity>` staging model (see
   [02-staging-layer.md](02-staging-layer.md)) with the raw country value cleaned up
   (trimmed/cast) but **not yet conformed** — call the column `country_raw`.
2. Copy `models/intermediate/conformed/country/examples/int_demo_source_country__resolved.sql`
   to `models/intermediate/conformed/country/int_<source>_country__resolved.sql`, point
   `source_rows` at your staging model, and set `source_system` to your source's name.
3. If your source sends values that don't match the canonical seed directly and aren't
   already in `seed_country_aliases.csv`, add them there (if the mapping is a general
   fact about the country, e.g. a numeric ISO code) or to `seed_country_overrides.csv`
   with a `reason` (if it's a quirk specific to your source).
4. Add tests for the new resolver model (see the `_country_conformed.yml` pattern) and
   run `dbt build --select <your new model>+`.
5. Downstream domain intermediate models `ref()` your resolver model to get
   `country_iso3`, exactly like `int_customers__with_conformed_country.sql` does in
   [03-intermediate-layer-and-conformed-dimensions.md](03-intermediate-layer-and-conformed-dimensions.md).

## How to: add a manual override

Add a row to `seed_country_overrides.csv` with a `reason`, `updated_by`, and
`updated_at`. Leave `source_system` blank for a correction that should apply everywhere;
set it to a specific source name if the override is only valid for that source's data.
Re-run `dbt seed` and the affected resolver model(s) will pick it up automatically —
no SQL changes required. This is the entire point of separating overrides from aliases:
a one-off business decision shouldn't require a code review of Jinja/SQL, just a
reviewed CSV row.

## How to: conform a brand-new dimension type (e.g. currency)

1. Create `seeds/conformed_dimensions/currency/` with `seed_currency_codes.csv`,
   `seed_currency_aliases.csv`, `seed_currency_overrides.csv`, following the same
   column shape as the country seeds (canonical key + attributes / alias_value +
   canonical key / source_system + raw_value + canonical key + reason).
2. Add an entry to `vars.conformed_dimensions` in `dbt_project.yml`:

   ```yaml
   vars:
     conformed_dimensions:
       currency:
         codes_seed: seed_currency_codes
         aliases_seed: seed_currency_aliases
         overrides_seed: seed_currency_overrides
         canonical_key: currency_iso3
         match_columns: ['currency_iso3', 'currency_name']
         unknown_value: 'UNKNOWN'
   ```

3. Build `models/intermediate/conformed/currency/int_currency__conformed.sql` as a thin
   passthrough of `seed_currency_codes`, exactly like `int_country__conformed.sql`.
4. Build per-source resolver models calling
   `{{ conform_dimension_mapping(dimension='currency', source_system='...') }}` —
   identical shape to the country resolvers.

Nothing in `conform_dimension_mapping.sql` or `dimension_unknown_value.sql` needs to
change. That's the test of whether this framework is actually generic: if adding a
dimension ever requires touching the macro, something has drifted and should be raised
with the team before merging.

## Bringing this into the real warehouse project

This repo's `dbt_project.yml`/`seeds`/`macros`/`models` are a standalone reference you
can run in isolation. To adopt it in the real project: copy the `macros/conformed_dimensions/`
folder as-is, copy the `vars.conformed_dimensions` block into the real project's
`dbt_project.yml`, and copy the `seeds/conformed_dimensions/country/` seeds (minus the
`demo/` subfolder and its resolver model, which exist only to make this repo runnable on
its own).
