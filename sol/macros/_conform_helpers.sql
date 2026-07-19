{#
    Internal helpers for the conformance macros.

    Synapse Dedicated SQL Pool is the production target. DuckDB overrides keep
    the same public API so this repo can demonstrate the pattern locally.
#}

{% macro _conform_assert_namespace(joins) %}
    {%- if joins is none
          or joins.sql is not defined
          or joins.aliases is not defined
          or joins.alias_signatures is not defined
          or joins.rendered is not defined -%}
        {{ exceptions.raise_compiler_error(
            "conform_join: the first argument must come from conform_joins_ns()."
        ) }}
    {%- endif -%}
{% endmacro %}


{% macro _conform_assert_identifier(value, argument_name) %}
    {%- if value is none or (value | string | trim) == '' -%}
        {{ exceptions.raise_compiler_error(
            "conform: '" ~ argument_name ~ "' must be a non-empty SQL identifier."
        ) }}
    {%- endif -%}

    {%- if value | string | length > 128 -%}
        {{ exceptions.raise_compiler_error(
            "conform: '" ~ argument_name ~ "' exceeds Synapse's 128-character identifier limit."
        ) }}
    {%- endif -%}

    {%- if (value | string | first) in '0123456789' -%}
        {{ exceptions.raise_compiler_error(
            "conform: '" ~ argument_name ~ "' must start with a letter or underscore; got '"
            ~ value ~ "'."
        ) }}
    {%- endif -%}

    {%- set valid_chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' -%}
    {%- for char in value | string -%}
        {%- if char not in valid_chars -%}
            {{ exceptions.raise_compiler_error(
                "conform: '" ~ argument_name ~ "' must contain only letters, numbers, and underscores; got '"
                ~ value ~ "'."
            ) }}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}


{% macro _conform_assert_qualified_expression(value, argument_name='column') %}
    {%- if value is none or (value | string | trim) == '' -%}
        {{ exceptions.raise_compiler_error(
            "conform: '" ~ argument_name ~ "' must be a non-empty, relation-qualified SQL expression."
        ) }}
    {%- endif -%}
    {%- if '.' not in (value | string) -%}
        {{ exceptions.raise_compiler_error(
            "conform: '" ~ argument_name ~ "' must be relation-qualified (for example, 's.gender_conform_key'); got '"
            ~ value ~ "'."
        ) }}
    {%- endif -%}
{% endmacro %}


{% macro _conform_output_columns(output_columns) %}
    {%- if output_columns is string -%}
        {%- set columns = [output_columns] -%}
    {%- else -%}
        {%- set columns = output_columns -%}
    {%- endif -%}

    {%- if columns is none or columns | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "conform_lookup: 'output_columns' must contain at least one dimension column."
        ) }}
    {%- endif -%}

    {%- set seen = [] -%}
    {%- for column_name in columns -%}
        {%- do _conform_assert_identifier(column_name, 'output_columns') -%}
        {%- if (column_name | lower) in ['conform_key', 'conform_candidate_count', 'conform_rn'] -%}
            {{ exceptions.raise_compiler_error(
                "conform_lookup: output column '" ~ column_name ~ "' conflicts with a reserved column name."
            ) }}
        {%- endif -%}
        {%- if (column_name | lower) in seen -%}
            {{ exceptions.raise_compiler_error(
                "conform_lookup: duplicate output column '" ~ column_name ~ "'."
            ) }}
        {%- endif -%}
        {%- do seen.append(column_name | lower) -%}
    {%- endfor -%}

    {{ return(columns) }}
{% endmacro %}


{% macro _conform_sql_string_literal(value) -%}
{%- if target.type == 'duckdb' -%}
'{{ value | string | replace("'", "''") }}'
{%- else -%}
N'{{ value | string | replace("'", "''") }}'
{%- endif -%}
{%- endmacro %}


{#
    Canonical Unicode normalisation. Change this logic only through a
    controlled full refresh of every lookup and source model that persists
    conform_key.
#}
{% macro _conform_normalized_value_expr(expr, length=3500) -%}
{%- if target.type == 'duckdb' -%}
nullif(upper(trim(cast({{ expr }} as varchar))), '')
{%- else -%}
nullif(upper(ltrim(rtrim(convert(nvarchar({{ length }}), {{ expr }})))), N'')
{%- endif -%}
{%- endmacro %}


{#
    Build a fixed-width, Unicode-safe key. SHA-256 produces a 32-byte binary
    join key regardless of the source data width. When a source system is
    supplied, char(31) separates it from the value before hashing; source-system
    identifiers must not contain that control character.

    Hashing happens when staging and lookup tables are built, never in the
    normal downstream join path. The ambiguity test also detects any practical
    key collision by observing more than one candidate for a persisted key.
#}
{% macro _conform_key_expr(value_expr, source_system_expr=none) -%}
    {%- set normalized_value = _conform_normalized_value_expr(value_expr, 3500) | trim -%}
    {%- if source_system_expr is none -%}
case
    when {{ normalized_value }} is null then null
    {%- if target.type == 'duckdb' %}
    else sha256({{ normalized_value }})
    {%- else %}
    else convert(binary(32), hashbytes('SHA2_256', {{ normalized_value }}))
    {%- endif %}
end
    {%- else -%}
        {%- set normalized_system = _conform_normalized_value_expr(source_system_expr, 128) | trim -%}
case
    when {{ normalized_value }} is null or {{ normalized_system }} is null then null
    {%- if target.type == 'duckdb' %}
    else sha256(concat({{ normalized_system }}, chr(31), {{ normalized_value }}))
    {%- else %}
    else convert(
        binary(32),
        hashbytes('SHA2_256', concat({{ normalized_system }}, nchar(31), {{ normalized_value }}))
    )
    {%- endif %}
end
    {%- endif -%}
{%- endmacro %}


{% macro _conform_fallback(output_column, default) %}
    {%- if default is not none -%}
        {{ return(default) }}
    {%- elif (output_column | lower).endswith('_sk') -%}
        {{ return('-1') }}
    {%- else -%}
        {# Non-key outputs can be any type, so NULL is the only safe default. #}
        {{ return('null') }}
    {%- endif -%}
{% endmacro %}


{% macro _conform_count_star() -%}
{%- if target.type == 'duckdb' -%}
count(*)
{%- else -%}
count_big(*)
{%- endif -%}
{%- endmacro %}
