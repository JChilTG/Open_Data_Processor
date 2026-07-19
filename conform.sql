{#
    conform: conform a column in the current model to a canonical dimension.

    Arguments
        column        field in the current model to conform
        dim           canonical dimension model to conform to (e.g. 'dim_gender')
        join_column   column in the dimension that `column` matches on
        output_column column to return from the dimension.
                      Default: the dimension's column ending in '_sk'
        mapping       optional seed that translates raw source values first.
                      Columns: source_value, canonical_value [, source_system]
                      canonical_value must match dim.join_column values
        source_system optional filter on the mapping seed
        default       fallback for unmatched values.
                      Default: -1 for *_sk outputs, 'UNKNOWN' otherwise

    Usage in a model select list:

        select
            -- values already match dim codes; returns gender_sk (auto-detected)
            {{ conform('gender_code', 'dim_gender', 'gender_code') }}   as gender_sk,

            -- return a different column from the dimension
            {{ conform('gender_code', 'dim_gender', 'gender_code',
                       output_column='gender_name') }}                  as gender_name,

            -- raw values need translating via a seed first
            {{ conform('raw_gender', 'dim_gender', 'gender_code',
                       mapping='map_gender') }}                         as gender_sk
        from {{ ref('stg_employee') }}

    Compiles to an inline CASE (Synapse-safe: no joins / no correlated subqueries).
    Best when the lookup is small (low dozens of distinct values) so the CASE
    stays compact.

    For larger dims on Synapse dedicated SQL pools, use conform_join instead
    (ANSI LEFT JOINs against REPLICATE dims).

    ref() calls register the dim and seed as dependencies, so dbt build
    runs them before compiling any model that calls this.
#}

{% macro conform(column, dim, join_column, output_column=none, mapping=none, source_system=none, default=none) %}

    {%- set dim_relation = ref(dim) -%}
    {%- set map_relation = ref(mapping) if mapping else none -%}
    {%- set out_col = _conform_resolve_output_column(dim_relation, output_column, dim) -%}

    {%- if execute and out_col is not none -%}

        {%- set is_key = (out_col | lower).endswith('_sk') -%}
        {%- set fallback = _conform_fallback(out_col, default) -%}

        {%- if map_relation -%}
            {#- raw value -> seed -> dim: translate then resolve output -#}
            {%- set sql -%}
                select {{ _conform_key_expr('m.source_value') }} as conform_key, d.{{ out_col }} as conform_value
                from {{ map_relation }} m
                inner join {{ dim_relation }} d
                    on {{ _conform_key_expr('d.' ~ join_column) }} = {{ _conform_key_expr('m.canonical_value') }}
                {%- if source_system %}
                where m.source_system = '{{ source_system }}'
                {%- endif %}
            {%- endset -%}
        {%- else -%}
            {#- raw value matches dim.join_column directly -#}
            {%- set sql -%}
                select {{ _conform_key_expr('d.' ~ join_column) }} as conform_key, d.{{ out_col }} as conform_value
                from {{ dim_relation }} d
            {%- endset -%}
        {%- endif -%}

        {%- set pairs = run_query(sql) -%}

        case
        {%- for row in pairs.rows %}
            when {{ _conform_key_expr(column) }} = '{{ row[0] | replace("'", "''") }}'
                then {% if is_key %}{{ row[1] }}{% else %}'{{ row[1] | replace("'", "''") }}'{% endif %}
        {%- endfor %}
            else {{ fallback }}
        end

    {%- else -%}
        {#- parse-time placeholder; never executed -#}
        null
    {%- endif -%}

{% endmacro %}
