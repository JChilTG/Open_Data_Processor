{% macro conform_dimension_mapping(dimension, source_system=none) %}
{#-
    Builds a raw_value -> canonical key lookup for a conformed dimension, from three
    inputs (highest precedence first):

      1. source-specific manual override   seed_<dim>_overrides, source_system = <source_system>
      2. global manual override            seed_<dim>_overrides, source_system is null
      3. known alias                       seed_<dim>_aliases (2-char/3-char/numeric/
                                            historical-name variants that don't match
                                            the canonical seed's own columns)
      4. direct match                      against the canonical seed's own code/name
                                            columns (handles the common case: a source
                                            already sends a valid iso2, iso3, or exact
                                            country_name and needs no special-casing)

    Returns two columns: `raw_value` (normalized via normalize_raw_value(), see that
    macro) and the dimension's canonical key column (e.g. `country_iso3`) — one row per
    raw value known to any of the three inputs above. Raw values with NO match anywhere
    (typos, garbage, a country that was renamed, ...) simply won't appear in the result;
    the calling model is responsible for coalescing the join result to the dimension's
    configured `unknown_value` (see `dimension_unknown_value()` in this same folder) so
    a bad raw value degrades to "Unknown" instead of a null that breaks a downstream
    join.

    Written specifically for Synapse dedicated SQL pool:
      - Precedence is implemented with ROW_NUMBER() OVER (PARTITION BY ... ORDER BY
        priority), not OFFSET/FETCH or QUALIFY — dedicated SQL pools support neither
        (window functions like ROW_NUMBER are fully supported, so that's what this
        builds on).
      - Every raw-value and canonical-key comparison goes through normalize_raw_value()
        / an explicit `collate database_default`, so a resolver model's join to this
        mapping never fails with "Cannot resolve the collation conflict" against a
        bronze-sourced column — see that macro's comment for why this is the single
        most likely real-world failure mode here.

    Params:
      dimension: key into the `conformed_dimensions` dict in dbt_project.yml vars.
      source_system: optional source system name (e.g. 'salesforce'). Enables
        source-specific overrides to outrank global ones. Omit when the dimension
        isn't being resolved for one particular source.

    Adding a new dimension type does not require touching this macro: add
    seed_<dim>_codes / seed_<dim>_aliases / seed_<dim>_overrides seeds and one entry
    under `vars.conformed_dimensions` in dbt_project.yml, then call this macro with the
    new `dimension` name.

    Usage (inside a per-source resolver model) — normalize the source's raw column
    with the same helper so both sides of the join match on identical logic:

        with source_rows as (
            select
                stg.salesforce_account_id,
                {{ normalize_raw_value('stg.country_raw') }} as raw_value
            from {{ ref('stg_salesforce__accounts') }} as stg
        ),

        value_map as (
            {{ conform_dimension_mapping(dimension='country', source_system='salesforce') }}
        )

        select
            source_rows.salesforce_account_id,
            coalesce(value_map.country_iso3, {{ dimension_unknown_value('country') }}) as country_iso3
        from source_rows
        left join value_map
            on source_rows.raw_value = value_map.raw_value
-#}
{%- set dim_config = var('conformed_dimensions')[dimension] -%}
{%- set canonical_key = dim_config['canonical_key'] -%}
{%- set match_columns = dim_config['match_columns'] -%}
{%- set codes_seed = dim_config['codes_seed'] -%}
{%- set aliases_seed = dim_config['aliases_seed'] -%}
{%- set overrides_seed = dim_config['overrides_seed'] -%}

with candidates as (

    {%- if source_system %}
    select
        {{ normalize_raw_value('raw_value') }} as raw_value,
        {{ canonical_key }} collate database_default as {{ canonical_key }},
        1 as priority
    from {{ ref(overrides_seed) }}
    where source_system = '{{ source_system }}'

    union all

    {%- endif %}
    select
        {{ normalize_raw_value('raw_value') }} as raw_value,
        {{ canonical_key }} collate database_default as {{ canonical_key }},
        2 as priority
    from {{ ref(overrides_seed) }}
    where source_system is null

    union all

    select
        {{ normalize_raw_value('alias_value') }} as raw_value,
        {{ canonical_key }} collate database_default as {{ canonical_key }},
        3 as priority
    from {{ ref(aliases_seed) }}

    {%- for col in match_columns %}

    union all

    select
        {{ normalize_raw_value(col) }} as raw_value,
        {{ canonical_key }} collate database_default as {{ canonical_key }},
        4 as priority
    from {{ ref(codes_seed) }}
    {%- endfor %}

),

ranked as (

    select
        raw_value,
        {{ canonical_key }},
        row_number() over (
            partition by raw_value
            order by priority
        ) as _priority_rank

    from candidates
    where raw_value is not null

)

select
    raw_value,
    {{ canonical_key }}
from ranked
where _priority_rank = 1

{% endmacro %}
