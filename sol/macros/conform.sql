{#
    Synapse Dedicated SQL Pool conformance pattern
    =================================================

    Design goals
      * no run_query() during dbt compilation
      * no generated CASE lists
      * no per-consumer ROW_NUMBER(), dimension scan, or mapping scan
      * one narrow, pre-materialized lookup built with CTAS
      * one equality join per distinct lookup/key pair, reused automatically
      * deterministic duplicate handling plus an auditable candidate count

    1. Build a lookup model once
    ----------------------------

        -- models/lookups/lkp_gender.sql
        {{ conform_lookup_config(size='small') }}

        {{ conform_lookup(
            dim='dim_gender_current',
            join_column='gender_code',
            output_columns=['gender_sk', 'gender_name'],
            mapping='map_gender',
            mapping_source_system_column='source_system',
            dedupe_order_by='d.gender_sk'  -- finish with a stable unique key
        ) }}

    Use size='small' for lookup tables below the replicated-table threshold
    (normally below 2 GB compressed);
    it creates DISTRIBUTION = REPLICATE with a clustered index on conform_key.
    Use size='large' for a genuinely large lookup; it creates
    DISTRIBUTION = HASH(conform_key) with a clustered columnstore index. To
    eliminate data movement for that large lookup, the consuming table must
    also be HASH distributed on the same persisted conform_key and data type.

    Prefer a full table build (CTAS) for lookups. Replicated tables are costly
    to update incrementally, and dimensions/mappings normally change slowly.

    Add these tests to the lookup model and run consumers with `dbt build`:

        columns:
          - name: conform_key
            data_tests: [not_null, unique]
        data_tests:
          - conform_lookup_unambiguous

    The unambiguous test fails if the lookup builder had to choose between
    multiple candidate rows. If duplicates are intentional, provide a business
    priority in dedupe_order_by and configure a less strict candidate-count
    test explicitly.

    2. Persist conformance keys in the source staging model
    --------------------------------------------------------

        {{ conform_key('s.raw_gender', source_system='ACE') }}
            as gender_conform_key

    Materialize this staging model as a table. The resulting conform_key is a
    fixed binary(32), so downstream joins, statistics, and HASH distribution
    stay narrow and Unicode-safe. For direct dimension matching, omit
    source_system.

    3. Consume the lookup with one simple join
    --------------------------------------------

        {% set cj = conform_joins_ns() %}

        select
            e.employee_id,
            {{ conform_join(cj, 'e.gender_conform_key', 'lkp_gender', 'gender_sk') }}
                as gender_sk,
            {{ conform_join(cj, 'e.gender_conform_key', 'lkp_gender', 'gender_name',
                            default="cast(N'UNKNOWN' as nvarchar(100))") }}
                as gender_name
        from {{ ref('stg_employee') }} as e
        {{ conform_joins(cj) }}

    Both selected values reuse the same physical join.
#}


{% macro conform_lookup_config(
    size='small',
    key_column='conform_key',
    index=none,
    create_statistics=none
) %}
    {%- do _conform_assert_identifier(key_column, 'key_column') -%}
    {%- set lookup_size = size | lower | trim -%}

    {%- if lookup_size == 'small' -%}
        {%- set distribution = 'REPLICATE' -%}
        {%- set resolved_index = index if index is not none else 'CLUSTERED INDEX (' ~ key_column ~ ')' -%}
    {%- elif lookup_size == 'large' -%}
        {%- set distribution = 'HASH(' ~ key_column ~ ')' -%}
        {%- set resolved_index = index if index is not none else 'CLUSTERED COLUMNSTORE INDEX' -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "conform_lookup_config: size must be 'small' or 'large'; got '" ~ size ~ "'."
        ) }}
    {%- endif -%}

    {# A clustered rowstore index already owns key statistics. Add explicit
       column statistics for the large CCI path unless the caller overrides. #}
    {%- set add_statistics = (lookup_size == 'large') if create_statistics is none else create_statistics -%}
    {%- set statistics_hook =
        'create statistics [st_conform_lookup_key] on {{ this }} ([' ~ key_column ~ '])'
    -%}

    {%- if target.type == 'duckdb' -%}
        {{ config(materialized='table') }}
    {%- elif add_statistics -%}
        {{ config(
            materialized='table',
            dist=distribution,
            index=resolved_index,
            post_hook=[statistics_hook]
        ) }}
    {%- else -%}
        {{ config(
            materialized='table',
            dist=distribution,
            index=resolved_index
        ) }}
    {%- endif -%}
{% endmacro %}


{% macro conform_key(column, source_system=none, source_system_column=none) %}
    {%- if source_system is not none and source_system_column is not none -%}
        {{ exceptions.raise_compiler_error(
            "conform_key: pass either source_system or source_system_column, not both."
        ) }}
    {%- endif -%}

    {%- if source_system_column is not none -%}
        {{ _conform_key_expr(column, source_system_column) }}
    {%- elif source_system is not none -%}
        {{ _conform_key_expr(column, _conform_sql_string_literal(source_system) | trim) }}
    {%- else -%}
        {{ _conform_key_expr(column) }}
    {%- endif -%}
{% endmacro %}


{% macro conform_lookup(
    dim,
    join_column,
    output_columns,
    mapping=none,
    mapping_source_column='source_value',
    mapping_canonical_column='canonical_value',
    mapping_source_system_column=none,
    dedupe_order_by=none
) %}
    {# ref() remains outside execute so dbt always records every dependency. #}
    {%- set dim_relation = ref(dim) -%}
    {%- set mapping_relation = ref(mapping) if mapping is not none else none -%}
    {%- set outputs = _conform_output_columns(output_columns) -%}

    {%- do _conform_assert_identifier(join_column, 'join_column') -%}
    {%- for output_column in outputs -%}
        {%- do _conform_assert_identifier(output_column, 'output_columns') -%}
    {%- endfor -%}

    {%- if mapping is not none -%}
        {%- do _conform_assert_identifier(mapping_source_column, 'mapping_source_column') -%}
        {%- do _conform_assert_identifier(mapping_canonical_column, 'mapping_canonical_column') -%}
        {%- if mapping_source_system_column is not none -%}
            {%- do _conform_assert_identifier(mapping_source_system_column, 'mapping_source_system_column') -%}
        {%- endif -%}
    {%- elif mapping_source_system_column is not none -%}
        {{ exceptions.raise_compiler_error(
            "conform_lookup: mapping_source_system_column requires a mapping model."
        ) }}
    {%- endif -%}

    {%- set d_join = 'd.' ~ adapter.quote(join_column) -%}

    {%- if mapping is not none -%}
        {%- set m_source = 'm.' ~ adapter.quote(mapping_source_column) -%}
        {%- set m_canonical = 'm.' ~ adapter.quote(mapping_canonical_column) -%}
        {%- if mapping_source_system_column is not none -%}
            {%- set m_system = 'm.' ~ adapter.quote(mapping_source_system_column) -%}
            {%- set lookup_key = _conform_key_expr(m_source, m_system) | trim -%}
        {%- else -%}
            {%- set lookup_key = _conform_key_expr(m_source) | trim -%}
        {%- endif -%}
    {%- else -%}
        {%- set lookup_key = _conform_key_expr(d_join) | trim -%}
    {%- endif -%}

    {%- if dedupe_order_by is not none and (dedupe_order_by | trim) != '' -%}
        {%- set ordering = dedupe_order_by | trim -%}
    {%- else -%}
        {%- set ordering_parts = [] -%}
        {%- for output_column in outputs -%}
            {%- do ordering_parts.append('d.' ~ adapter.quote(output_column)) -%}
        {%- endfor -%}
        {%- if mapping is not none -%}
            {%- do ordering_parts.append(m_canonical) -%}
            {%- do ordering_parts.append(m_source) -%}
        {%- else -%}
            {%- do ordering_parts.append(d_join) -%}
        {%- endif -%}
        {%- set ordering = ordering_parts | join(', ') -%}
    {%- endif -%}

with conform_ranked as (
    select
        {{ lookup_key }} as conform_key,
        {%- for output_column in outputs %}
        d.{{ adapter.quote(output_column) }} as {{ adapter.quote(output_column) }},
        {%- endfor %}
        {{ _conform_count_star() }} over (
            partition by {{ lookup_key }}
        ) as conform_candidate_count,
        row_number() over (
            partition by {{ lookup_key }}
            order by {{ ordering }}
        ) as conform_rn
    {%- if mapping is not none %}
    from {{ mapping_relation }} as m
    inner join {{ dim_relation }} as d
        on {{ _conform_key_expr(d_join) }} = {{ _conform_key_expr(m_canonical) }}
    where {{ _conform_normalized_value_expr(m_source) }} is not null
        {%- if mapping_source_system_column is not none %}
      and {{ _conform_normalized_value_expr(m_system, 128) }} is not null
        {%- endif %}
    {%- else %}
    from {{ dim_relation }} as d
    where {{ _conform_normalized_value_expr(d_join) }} is not null
    {%- endif %}
)
select
    conform_key,
    {%- for output_column in outputs %}
    {{ adapter.quote(output_column) }},
    {%- endfor %}
    conform_candidate_count
from conform_ranked
where conform_rn = 1
{% endmacro %}


{% macro conform_joins_ns() %}
    {{ return(namespace(
        sql=[],
        n=0,
        aliases={},
        alias_signatures={},
        rendered=false
    )) }}
{% endmacro %}


{% macro conform_joins(joins) %}
    {%- do _conform_assert_namespace(joins) -%}
    {%- if joins.rendered -%}
        {{ exceptions.raise_compiler_error(
            "conform_joins: this join namespace has already been rendered."
        ) }}
    {%- endif -%}
    {%- set joins.rendered = true -%}

    {% for join_sql in joins.sql %}
{{ join_sql | trim }}{{ '\n' }}
    {% endfor %}
{% endmacro %}


{% macro conform_join(
    joins,
    column,
    lookup,
    output_column,
    default=none,
    alias=none,
    normalize=false,
    source_system=none,
    source_system_column=none
) %}
    {%- do _conform_assert_namespace(joins) -%}
    {%- if joins.rendered -%}
        {{ exceptions.raise_compiler_error(
            "conform_join: call conform_join() in the select list before rendering conform_joins()."
        ) }}
    {%- endif -%}
    {%- do _conform_assert_qualified_expression(column) -%}
    {%- do _conform_assert_identifier(output_column, 'output_column') -%}

    {%- if alias is not none -%}
        {%- do _conform_assert_identifier(alias, 'alias') -%}
    {%- endif -%}
    {%- if not normalize and (source_system is not none or source_system_column is not none) -%}
        {{ exceptions.raise_compiler_error(
            "conform_join: source_system options require normalize=true. When column is a persisted conform key, its source system is already encoded."
        ) }}
    {%- endif -%}
    {%- if source_system is not none and source_system_column is not none -%}
        {{ exceptions.raise_compiler_error(
            "conform_join: pass either source_system or source_system_column, not both."
        ) }}
    {%- endif -%}

    {# ref() is unconditional so parse-time DAG discovery remains correct. #}
    {%- set lookup_relation = ref(lookup) -%}

    {%- if normalize -%}
        {%- if source_system_column is not none -%}
            {%- set join_expression = _conform_key_expr(column, source_system_column) | trim -%}
        {%- elif source_system is not none -%}
            {%- set join_expression = _conform_key_expr(
                column,
                _conform_sql_string_literal(source_system) | trim
            ) | trim -%}
        {%- else -%}
            {%- set join_expression = _conform_key_expr(column) | trim -%}
        {%- endif -%}
    {%- else -%}
        {%- set join_expression = column | trim -%}
    {%- endif -%}

    {%- set join_signature = (lookup | string) ~ '|' ~ join_expression -%}

    {%- if join_signature in joins.aliases -%}
        {%- set join_alias = joins.aliases[join_signature] -%}
        {%- if alias is not none and alias != join_alias -%}
            {{ exceptions.raise_compiler_error(
                "conform_join: lookup/key pair is already joined as '" ~ join_alias
                ~ "'; it cannot also use alias '" ~ alias ~ "'."
            ) }}
        {%- endif -%}
    {%- else -%}
        {%- set joins.n = joins.n + 1 -%}
        {%- set join_alias = alias if alias is not none else 'cj_' ~ joins.n -%}

        {%- if join_alias in joins.alias_signatures -%}
            {{ exceptions.raise_compiler_error(
                "conform_join: alias '" ~ join_alias ~ "' is already used by a different lookup join."
            ) }}
        {%- endif -%}

        {%- set join_sql -%}
left join {{ lookup_relation }} as {{ adapter.quote(join_alias) }}
    on {{ adapter.quote(join_alias) }}.{{ adapter.quote('conform_key') }} = {{ join_expression }}
        {%- endset -%}

        {%- do joins.sql.append(join_sql) -%}
        {%- do joins.aliases.update({join_signature: join_alias}) -%}
        {%- do joins.alias_signatures.update({join_alias: join_signature}) -%}
    {%- endif -%}

    {%- set value_expression = adapter.quote(join_alias) ~ '.' ~ adapter.quote(output_column) -%}
    {%- set fallback = _conform_fallback(output_column, default) -%}
    coalesce({{ value_expression }}, {{ fallback }})
{% endmacro %}


{# Short public alias for conform_join(). #}
{% macro conform(
    joins,
    column,
    lookup,
    output_column,
    default=none,
    alias=none,
    normalize=false,
    source_system=none,
    source_system_column=none
) %}
    {{ conform_join(
        joins=joins,
        column=column,
        lookup=lookup,
        output_column=output_column,
        default=default,
        alias=alias,
        normalize=normalize,
        source_system=source_system,
        source_system_column=source_system_column
    ) }}
{% endmacro %}


{% test conform_lookup_unambiguous(model, candidate_count_column='conform_candidate_count', max_candidates=1) %}
    {%- do _conform_assert_identifier(candidate_count_column, 'candidate_count_column') -%}
select
    conform_key,
    {{ adapter.quote(candidate_count_column) }}
from {{ model }}
where conform_key is null
   or {{ adapter.quote(candidate_count_column) }} > {{ max_candidates }}
{% endtest %}
