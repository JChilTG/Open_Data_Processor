{% macro dimension_unknown_value(dimension) %}
{#-
    Returns the configured fallback key (as a quoted SQL literal) for a conformed
    dimension, e.g. dimension_unknown_value('country') -> 'UNKNOWN'.

    Use this to coalesce a resolver model's join result rather than hardcoding the
    sentinel string in every model:

        coalesce(value_map.country_iso3, {{ dimension_unknown_value('country') }}) as country_iso3

    The sentinel must exist as a real row in the dimension's canonical seed (see
    seed_country_codes.csv's `UNKNOWN` row) so that marts built on top of this can still
    join a fact's foreign key to a real dimension row instead of a dangling null —
    see 08-conformed-dimension-framework.md.
-#}
{%- set dim_config = var('conformed_dimensions')[dimension] -%}
'{{ dim_config['unknown_value'] }}'
{%- endmacro %}
