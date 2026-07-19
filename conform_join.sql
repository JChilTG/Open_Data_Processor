{#
    conform_join — Synapse dedicated SQL pool–safe conform via ANSI LEFT JOINs.

    Why not a scalar subquery / APPLY?
        Correlated subqueries are a known anti-pattern on dedicated SQL pools
        (nested-loop style plans, high CPU). This macro emits set-based
        LEFT JOINs so the MPP optimizer can broadcast/hash a small REPLICATE
        lookup against the fact.

    Reliability
        - Lookup is deduped with ROW_NUMBER so join fan-out cannot inflate grain
        - Join key is precomputed on the (small) lookup side as `conform_key`
        - Deterministic aliases (no random) for stable compiled SQL
        - Same defaults as conform: -1 for *_sk, 'UNKNOWN' otherwise

    Performance (dedicated SQL pool)
        - Materialize `dim` / mapping seeds as REPLICATE (see dim_gender)
        - Keep lookup tables small; prefer `conform` (CASE) only when the
          compiled CASE stays tiny (low dozens of distinct values)
        - upper/ltrim/rtrim on the fact column is applied once per join;
          prefer storing canonical codes already normalized when possible
        - Keep statistics updated on fact + dims

    Usage (two-step: register in SELECT, render joins after FROM):

        {% set joins = conform_joins_ns() %}

        select
            e.employee_id,
            {{ conform_join(joins, 'e.gender_code', 'dim_gender', 'gender_code') }}
                as gender_sk,
            {{ conform_join(joins, 'e.raw_gender', 'dim_gender', 'gender_code',
                            mapping='map_gender', source_system='hr_system') }}
                as gender_sk_hr
        from {{ ref('stg_employee') }} e
        {{ conform_joins(joins) }}

    Arguments (same semantics as conform)
        joins         namespace from conform_joins_ns()
        column        qualified fact column, e.g. 'e.gender_code'
        dim           dimension model name
        join_column   dim column to match
        output_column default: sole *_sk on dim
        mapping       optional seed (source_value, canonical_value [, source_system])
        source_system optional mapping filter
        default       unmatched fallback
        alias         optional join alias (auto cj_1, cj_2, ...)
#}


{% macro conform_joins_ns() %}
    {{ return(namespace(sql=[], n=0)) }}
{% endmacro %}


{% macro conform_joins(joins) %}
    {%- for join_sql in joins.sql %}
    {{ join_sql }}
    {%- endfor %}
{% endmacro %}


{% macro conform_join(joins, column, dim, join_column, output_column=none, mapping=none, source_system=none, default=none, alias=none) %}

    {%- if joins is none or joins.sql is not defined -%}
        {{ exceptions.raise_compiler_error(
            "conform_join: pass a namespace from conform_joins_ns() as the first argument, "
            ~ "and render it with {{ conform_joins(joins) }} after FROM."
        ) }}
    {%- endif -%}

    {%- if '.' not in (column | string) -%}
        {{ exceptions.raise_compiler_error(
            "conform_join: column '" ~ column ~ "' must be relation-qualified "
            ~ "(e.g. 'e." ~ column ~ "')."
        ) }}
    {%- endif -%}

    {%- set dim_relation = ref(dim) -%}
    {%- set map_relation = ref(mapping) if mapping else none -%}
    {%- set out_col = _conform_resolve_output_column(dim_relation, output_column, dim) -%}

    {%- set joins.n = joins.n + 1 -%}
    {%- set join_alias = alias if alias is not none else ('cj_' ~ joins.n) -%}

    {%- if execute and out_col is not none -%}
        {%- set fallback = _conform_fallback(out_col, default) -%}

        {%- if map_relation -%}
            {%- set join_sql -%}
        left join (
            select
                conform_value,
                conform_key
            from (
                select
                    d.{{ out_col }} as conform_value,
                    {{ _conform_key_expr('m.source_value') }} as conform_key,
                    row_number() over (
                        partition by {{ _conform_key_expr('m.source_value') }}
                        order by m.source_value
                    ) as conform_rn
                from {{ map_relation }} as m
                inner join {{ dim_relation }} as d
                    on {{ _conform_key_expr('d.' ~ join_column) }}
                     = {{ _conform_key_expr('m.canonical_value') }}
                {%- if source_system %}
                where m.source_system = '{{ source_system }}'
                {%- endif %}
            ) as ranked
            where conform_rn = 1
        ) as {{ join_alias }}
            on {{ join_alias }}.conform_key = {{ _conform_key_expr(column) }}
            {%- endset -%}
        {%- else -%}
            {%- set join_sql -%}
        left join (
            select
                conform_value,
                conform_key
            from (
                select
                    d.{{ out_col }} as conform_value,
                    {{ _conform_key_expr('d.' ~ join_column) }} as conform_key,
                    row_number() over (
                        partition by {{ _conform_key_expr('d.' ~ join_column) }}
                        order by d.{{ out_col }}
                    ) as conform_rn
                from {{ dim_relation }} as d
            ) as ranked
            where conform_rn = 1
        ) as {{ join_alias }}
            on {{ join_alias }}.conform_key = {{ _conform_key_expr(column) }}
            {%- endset -%}
        {%- endif -%}

        {%- do joins.sql.append(join_sql) -%}
        coalesce({{ join_alias }}.conform_value, {{ fallback }})

    {%- else -%}
        null
    {%- endif -%}

{% endmacro %}
