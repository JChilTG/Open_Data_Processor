{#
  Create column statistics on a table in an Azure Synapse Dedicated SQL Pool.

  Why this matters on Synapse:
    The dedicated SQL pool optimizer leans heavily on statistics to choose join
    order, distribution movement, and memory grants. Unlike most warehouses it
    does NOT auto-create stats on every column by default, so stale or missing
    stats are one of the most common causes of slow models. Creating stats on
    join keys, filter predicates, and distribution columns right after a model
    builds keeps plans healthy.

  Designed to be used as a post-hook so stats are (re)created every build:

    {{ config(
        post_hook="{{ create_statistics(['account_id', 'snapshot_date']) }}"
    ) }}

  Behaviour:
    - Idempotent: each statistic is only created if it does not already exist
      (checked via sys.stats), so it is safe to run on every build. To force a
      refresh, drop the stat first or run `update statistics` separately.
    - Single-column stats: pass a list of column names.
    - Composite (multi-column) stats: pass a nested list, e.g.
      ['account_id', ['region', 'industry']] creates one stat on account_id and
      one composite stat on (region, industry).

  Args:
    columns   list of column names (string) and/or column groups (list of
              strings) to build statistics on.
    relation  the relation to target. Defaults to `this` (the current model).
              Pass a Relation object (this / ref() / source()), not a raw string.
    scan      'default' (sampled, the engine decides), 'fullscan', or an integer
              N to use WITH SAMPLE N PERCENT.
#}
{% macro create_statistics(columns, relation=none, scan='default') %}
    {%- set relation = relation or this -%}
    {%- set rel = relation.include(database=false) -%}

    {%- if columns is string -%}
        {%- set columns = [columns] -%}
    {%- endif -%}

    {%- if scan == 'fullscan' -%}
        {%- set scan_clause = ' with fullscan' -%}
    {%- elif scan is number -%}
        {%- set scan_clause = ' with sample ' ~ scan ~ ' percent' -%}
    {%- else -%}
        {%- set scan_clause = '' -%}
    {%- endif -%}

    {%- for entry in columns -%}
        {%- if entry is string -%}
            {%- set col_list = [entry] -%}
        {%- else -%}
            {%- set col_list = entry -%}
        {%- endif -%}

        {%- set stat_name = (
            'stat_' ~ relation.identifier ~ '_' ~ (col_list | join('_'))
        ) | replace(' ', '_') -%}

        {%- set quoted_cols = [] -%}
        {%- for c in col_list -%}
            {%- do quoted_cols.append(adapter.quote(c | trim)) -%}
        {%- endfor -%}

if not exists (
    select 1
    from sys.stats
    where name = '{{ stat_name }}'
      and object_id = object_id('{{ rel }}')
)
    create statistics {{ adapter.quote(stat_name) }}
        on {{ rel }} ({{ quoted_cols | join(', ') }}){{ scan_clause }};
    {% endfor -%}
{% endmacro %}
