{#
  Resolve a source's country identifier to a canonical ISO3 code, for Azure
  Synapse Dedicated SQL Pool (T-SQL).

  Resolution order (highest priority first):
    1. Manual override  -- a matching row in the country_overrides seed
                           (override_type = 'source_map').
    2. Automatic match  -- the normalized source value equals the canonical
                           name / iso2 / iso3 in dim_country.
    3. Unmatched        -- canonical_iso3 is NULL and match_type = 'unmatched';
                           these rows surface exactly what needs an override.

  Args:
    source_relation    the source table/relation (source() or ref()).
    source_system      label used to look up overrides, e.g. 'AS', 'AF'.
    match_field        'name' | 'iso2' | 'iso3' -- the canonical field to match
                       on, and the override match_field to look up.
    source_key_column  the column in source_relation holding the value to resolve
                       (e.g. 'name' for AS, 'iso2' for AF).

  Synapse notes:
    - COLLATE DATABASE_DEFAULT is applied to every string comparison so joins
      across a seed and a source with different column collations do not raise
      "Cannot resolve the collation conflict".
    - Matching is case- and whitespace-insensitive via UPPER(LTRIM(RTRIM(...))).
#}
{% macro country_crosswalk(source_relation, source_system, match_field, source_key_column) %}

    {%- set coll = 'collate database_default' -%}
    {%- if match_field == 'name' -%}
        {%- set canonical_expr = 'can.name' -%}
    {%- elif match_field == 'iso2' -%}
        {%- set canonical_expr = 'can.iso2' -%}
    {%- elif match_field == 'iso3' -%}
        {%- set canonical_expr = 'can.iso3' -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "country_crosswalk: match_field must be 'name', 'iso2', or 'iso3', got '" ~ match_field ~ "'"
        ) }}
    {%- endif -%}

    {%- set norm_src = "upper(ltrim(rtrim(sv.source_value))) " ~ coll -%}
    {%- set norm_ov  = "upper(ltrim(rtrim(ov.source_value))) " ~ coll -%}
    {%- set norm_can = "upper(ltrim(rtrim(" ~ canonical_expr ~ "))) " ~ coll -%}

with source_values as (
    select distinct
        ltrim(rtrim(cast({{ source_key_column }} as varchar(400)))) as source_value
    from {{ source_relation }}
    where {{ source_key_column }} is not null
      and ltrim(rtrim(cast({{ source_key_column }} as varchar(400)))) <> ''
),

overrides as (
    select
        ltrim(rtrim(source_value)) as source_value,
        upper(ltrim(rtrim(canonical_iso3))) as canonical_iso3
    from {{ ref('country_overrides') }}
    where override_type = 'source_map'
      and source_system = '{{ source_system }}'
      and match_field = '{{ match_field }}'
),

resolved as (
    select
        sv.source_value,
        coalesce(ov.canonical_iso3, can.iso3) as canonical_iso3,
        case
            when ov.canonical_iso3 is not null then 'override'
            when can.iso3 is not null then 'auto'
            else 'unmatched'
        end as match_type
    from source_values as sv
    left join overrides as ov
        on {{ norm_src }} = {{ norm_ov }}
    left join {{ ref('dim_country') }} as can
        on {{ norm_src }} = {{ norm_can }}
)

select
    '{{ source_system }}' as source_system,
    '{{ match_field }}' as match_field,
    r.source_value,
    r.canonical_iso3,
    dc.iso2 as canonical_iso2,
    dc.name as canonical_name,
    r.match_type
from resolved as r
left join {{ ref('dim_country') }} as dc
    on r.canonical_iso3 {{ coll }} = dc.iso3 {{ coll }}

{% endmacro %}
