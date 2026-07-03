{#
  TEMPLATE: SCD Type 2 from a CDC / delta feed (a daily APPEND of only the rows
  that changed), for Azure Synapse Dedicated SQL Pool (T-SQL).

  Use this instead of dim_scd2_template when the source is an append log of
  changed rows rather than a full daily snapshot.

  Why it differs from the snapshot template:
    - Each appended row is already a change event, so a version boundary is simply
      "the next delta for that key". No cross-day hash comparison is required to
      detect change (an optional no-op collapse is available for feeds that emit
      unchanged rows).
    - Deletes CANNOT be inferred from absence (a key is absent on days it did not
      change). They must arrive as an explicit delete/operation flag, so there is
      no keys_on_latest_snapshot / key_last_seen logic here.
    - effective_to is the day (or instant) before the next delta for the key; the
      latest delta stays open at the high date. A delete delta becomes the latest
      version with is_deleted = 1 (filter is_current = 1 and is_deleted = 0 for
      the active current state).

  Source contract:
    One row per change with: the natural key, a change date/timestamp column, the
    changed attributes, and (optionally) a delete flag. If the feed can emit
    unchanged rows, provide a hash column to collapse them.

  Synapse specifics: REPLICATE dist, BIT flags, DATEADD date math, HASHBYTES SK.
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

{#- ===================== Template configuration ===================== -#}
{%- set source_relation = ref('stg_account_deltas') -%}
{%- set natural_key = 'account_id' -%}
{%- set change_date_column = 'change_date' -%}
{%- set grain = 'day' -%}                 {#- 'day' or 'timestamp' -#}
{%- set surrogate_key_name = 'account_sk' -%}
{%- set high_date = "cast('9999-12-31' as date)" -%}

{#- Optional delete handling: set to none to disable. -#}
{%- set delete_flag_column = 'operation' -%}
{%- set delete_flag_values = ['D', 'DELETE', 'deleted', '1'] -%}

{#- Optional no-op collapse: a hash column that drops consecutive identical rows.
    Set to none if every delta is guaranteed to be a real change. -#}
{%- set hash_column = none -%}

{#- Optional tiebreak to dedupe multiple rows for the same key+change_date
    (keeps the highest value). Set to none if the feed is already unique. -#}
{%- set tiebreak_column = none -%}
{#- ================================================================= -#}

{#- ---- derived expressions ---- -#}
{%- set next_change = "lead(" ~ change_date_column ~ ") over (partition by "
      ~ natural_key ~ " order by " ~ change_date_column ~ ")" -%}
{%- if grain == 'timestamp' -%}
    {%- set effective_from_expr = change_date_column -%}
    {%- set high_literal = "cast('9999-12-31T00:00:00' as datetime2(0))" -%}
    {%- set effective_to_expr = "coalesce(" ~ next_change ~ ", " ~ high_literal ~ ")" -%}
    {%- set sk_from_cast = "varchar(30)" -%}
{%- else -%}
    {%- set effective_from_expr = "cast(" ~ change_date_column ~ " as date)" -%}
    {%- set effective_to_expr = "coalesce(dateadd(day, -1, cast(" ~ next_change
          ~ " as date)), " ~ high_date ~ ")" -%}
    {%- set sk_from_cast = "varchar(10)" -%}
{%- endif -%}

{%- if delete_flag_column -%}
    {%- set _vals = [] -%}
    {%- for v in delete_flag_values -%}
        {%- do _vals.append("'" ~ (v | string | lower) ~ "'") -%}
    {%- endfor -%}
    {%- set delete_condition = "lower(cast(" ~ delete_flag_column
          ~ " as varchar(50))) in (" ~ (_vals | join(', ')) ~ ")" -%}
{%- else -%}
    {%- set delete_condition = "1 = 0" -%}
{%- endif -%}

{#- ---- final SELECT list: SK, dynamic passthrough, framework cols ---- -#}
{%- set exclude = [] -%}
{%- for c in [change_date_column, hash_column, delete_flag_column, tiebreak_column] -%}
    {%- if c -%}{%- do exclude.append(c | lower) -%}{%- endif -%}
{%- endfor -%}

{%- set select_list = [] -%}
{%- do select_list.append(
    "convert(varchar(64), hashbytes('SHA2_256', concat(" ~ natural_key
    ~ ", '|', cast(effective_from_date as " ~ sk_from_cast ~ "))), 2) as "
    ~ surrogate_key_name
) -%}
{%- if execute -%}
    {%- for col in adapter.get_columns_in_relation(source_relation) -%}
        {%- if col.name | lower not in exclude -%}
            {%- do select_list.append(adapter.quote(col.name)) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}
{%- do select_list.append("effective_from_date") -%}
{%- do select_list.append("effective_to_date") -%}
{%- do select_list.append("is_current") -%}
{%- do select_list.append("is_deleted") -%}
{%- do select_list.append(change_date_column ~ " as source_change_date") -%}

with deltas as (
    select * from {{ source_relation }}
),

{% if tiebreak_column %}
deduped as (
    select * from (
        select
            *,
            row_number() over (
                partition by {{ natural_key }}, {{ change_date_column }}
                order by {{ tiebreak_column }} desc
            ) as _rn
        from deltas
    ) d
    where d._rn = 1
),
{% set stage1 = 'deduped' %}
{% else %}
{% set stage1 = 'deltas' %}
{% endif %}

{% if hash_column %}
changes as (
    select * from (
        select
            *,
            lag({{ hash_column }}) over (
                partition by {{ natural_key }}
                order by {{ change_date_column }}
            ) as _prev_hash
        from {{ stage1 }}
    ) h
    where h._prev_hash is null or h.{{ hash_column }} <> h._prev_hash
),
{% set stage2 = 'changes' %}
{% else %}
{% set stage2 = stage1 %}
{% endif %}

versioned as (
    select
        {{ stage2 }}.*,
        {{ effective_from_expr }} as effective_from_date,
        {{ effective_to_expr }} as effective_to_date,
        cast(case when {{ next_change }} is null then 1 else 0 end as bit) as is_current,
        cast(case when {{ delete_condition }} then 1 else 0 end as bit) as is_deleted
    from {{ stage2 }}
),

final as (
    select
        {{ select_list | join(',\n        ') }}
    from versioned
)

select * from final
