{#
    Shared helpers for conform / conform_join.
#}

{% macro _conform_resolve_output_column(dim_relation, output_column, dim_name) %}
    {%- if output_column is not none -%}
        {{ return(output_column) }}
    {%- endif -%}

    {%- if not execute -%}
        {{ return(none) }}
    {%- endif -%}

    {%- set dim_cols = adapter.get_columns_in_relation(dim_relation) | map(attribute='name') | list -%}
    {%- set sk_cols = [] -%}
    {%- for col in dim_cols -%}
        {%- if (col | string | lower).endswith('_sk') -%}
            {%- do sk_cols.append(col) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if sk_cols | length == 1 -%}
        {{ return(sk_cols[0]) }}
    {%- elif sk_cols | length == 0 -%}
        {{ exceptions.raise_compiler_error("conform: no column ending in '_sk' found in " ~ dim_name ~ ". Pass output_column explicitly.") }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("conform: multiple *_sk columns in " ~ dim_name ~ " (" ~ sk_cols | join(', ') ~ "). Pass output_column explicitly.") }}
    {%- endif -%}
{% endmacro %}


{% macro _conform_fallback(out_col, default) %}
    {%- set is_key = (out_col | lower).endswith('_sk') -%}
    {{ return(default if default is not none else ('-1' if is_key else "'UNKNOWN'")) }}
{% endmacro %}


{# Normalize join keys the same way on every side (Synapse + DuckDB). #}
{% macro _conform_key_expr(expr) -%}
upper(ltrim(rtrim(cast({{ expr }} as varchar(200)))))
{%- endmacro %}
