{% macro attribute_columns(relation) %}
{#
    Business attribute column names in `relation` (i.e. every column except
    the fixed SCD2 plumbing columns and any bronze-layer metadata column,
    which by convention is prefixed with an underscore and carries no
    business meaning). Introspected live against the warehouse so new/removed
    source columns don't require editing this project.

    Only used by the opt-in deletion-detection branches in entity_scd2.sql,
    which need an explicit, order-stable column list to keep a UNION ALL
    aligned -- everywhere else in this project a plain `select *` is enough.
#}
{%- set system_columns = ['entity_id', '_landing_extract_date', 'attribute_hash', 'is_deleted'] -%}
{%- set columns = adapter.get_columns_in_relation(relation) -%}
{%- set attribute_cols = [] -%}
{%- for column in columns -%}
    {%- if column.column|lower not in system_columns and not column.column.startswith('_') -%}
        {%- do attribute_cols.append(column.column) -%}
    {%- endif -%}
{%- endfor -%}
{{ return(attribute_cols) }}
{% endmacro %}

{% macro column_list(cols, alias=none) -%}
{%- for col in cols -%}
{{ alias ~ '.' if alias }}{{ col }}{{ ", " if not loop.last }}
{%- endfor -%}
{%- endmacro %}
