{% macro star_except(relation, except=[], relation_alias=none) %}
{#
    A `select * except (...)` substitute for dedicated SQL pool, which -- as
    plain T-SQL -- has no EXCEPT-in-select-list syntax (that's BigQuery/
    Snowflake). Introspects `relation` live against the warehouse and returns
    a plain comma-separated column list with every name in `except` removed
    (case-insensitive), ready to drop straight into a select list:

        select {{ star_except(source('raw', 'my_table'), except=['_ingested_at', '_source_file']) }}
        from {{ source('raw', 'my_table') }}

        -- with a table alias, e.g. inside a join:
        select {{ star_except(ref('foo'), except=['updated_at'], relation_alias='f') }}
        from {{ ref('foo') }} f join ...

    Because this introspects the live relation, it needs a real warehouse
    connection even just to run `dbt compile` -- the same requirement as any
    macro built on adapter.get_columns_in_relation.
#}
{%- set exclude = except | map('lower') | list -%}
{%- set columns = adapter.get_columns_in_relation(relation) -%}
{%- set include_cols = [] -%}
{%- for column in columns -%}
    {%- if column.column|lower not in exclude -%}
        {%- do include_cols.append(column.column) -%}
    {%- endif -%}
{%- endfor -%}
{%- if include_cols | length == 0 -%}
    {{ exceptions.raise_compiler_error("star_except: every column of " ~ relation ~ " was excluded by " ~ except ~ " -- nothing left to select") }}
{%- endif -%}
{%- for col in include_cols -%}
{{ relation_alias ~ '.' if relation_alias }}{{ col }}{{ ", " if not loop.last }}
{%- endfor -%}
{% endmacro %}
