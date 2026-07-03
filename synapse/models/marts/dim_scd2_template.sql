{#
  TEMPLATE: SCD Type 2 dimension from daily full-table snapshots, for Azure
  Synapse Dedicated SQL Pool (T-SQL).

  This is the generic, reusable version of dim_account_scd2. To build an SCD2 for
  a new entity:
    1. Copy this file and rename it (e.g. dim_customer_scd2.sql).
    2. Edit only the "Template configuration" block below.
  Everything else is column-agnostic -- business columns are discovered at build
  time, so the model survives source schema changes (new columns flow in, removed
  columns drop out instead of erroring).

  Source contract:
    The source relation must have one row per (natural key, snapshot date) and a
    precomputed change-detection hash column (see the hash_all_except macro). The
    natural key, snapshot-date, and hash columns are the framework contract.

  Synapse specifics:
    - REPLICATE distribution for a small dimension.
    - No boolean type: is_current is CAST(CASE ... END AS BIT).
    - Date math via DATEADD; high date via CAST('9999-12-31' AS DATE).
    - HASHBYTES surrogate key (md5 has no T-SQL equivalent).
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

{#- ===================== Template configuration ===================== -#}
{%- set source_relation = ref('stg_d365_account_snapshots') -%}
{%- set natural_key = 'account_id' -%}
{%- set snapshot_date_column = 'snapshot_date' -%}
{%- set hash_column = 'attribute_hash' -%}
{%- set surrogate_key_name = 'account_sk' -%}
{%- set high_date = "cast('9999-12-31' as date)" -%}
{#- ================================================================= -#}

{#- control columns not carried into the dimension as attributes -#}
{%- set passthrough_exclude = [hash_column, 'previous_' ~ hash_column, snapshot_date_column] -%}
{%- set exclude_lower = passthrough_exclude | map('lower') | list -%}

{#- build the final SELECT list: SK, dynamic passthrough, then framework cols -#}
{%- set select_list = [] -%}
{%- do select_list.append(
    "convert(varchar(64), hashbytes('SHA2_256', concat(" ~ natural_key
    ~ ", '|', cast(effective_from_date as varchar(10)))), 2) as " ~ surrogate_key_name
) -%}
{%- if execute -%}
    {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
    {%- for col in source_columns -%}
        {%- if col.name | lower not in exclude_lower -%}
            {%- do select_list.append(adapter.quote(col.name)) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}
{%- do select_list.append("effective_from_date") -%}
{%- do select_list.append("effective_to_date") -%}
{%- do select_list.append(
    "cast(case when effective_to_date = " ~ high_date ~ " then 1 else 0 end as bit) as is_current"
) -%}
{%- do select_list.append(snapshot_date_column ~ " as source_snapshot_date") -%}

with snapshots as (
    select * from {{ source_relation }}
),

latest_snapshot as (
    select max({{ snapshot_date_column }}) as {{ snapshot_date_column }}
    from snapshots
),

keys_on_latest_snapshot as (
    select distinct snapshots.{{ natural_key }}
    from snapshots
    inner join latest_snapshot
        on snapshots.{{ snapshot_date_column }} = latest_snapshot.{{ snapshot_date_column }}
),

key_last_seen as (
    select
        {{ natural_key }},
        max({{ snapshot_date_column }}) as last_seen_date
    from snapshots
    group by {{ natural_key }}
),

with_previous_hash as (
    select
        *,
        lag({{ hash_column }}) over (
            partition by {{ natural_key }}
            order by {{ snapshot_date_column }}
        ) as previous_{{ hash_column }}
    from snapshots
),

change_points as (
    select *
    from with_previous_hash
    where
        previous_{{ hash_column }} is null
        or {{ hash_column }} <> previous_{{ hash_column }}
),

with_effective_dates as (
    select
        change_points.*,
        change_points.{{ snapshot_date_column }} as effective_from_date,
        coalesce(
            dateadd(day, -1, lead(change_points.{{ snapshot_date_column }}) over (
                partition by change_points.{{ natural_key }}
                order by change_points.{{ snapshot_date_column }}
            )),
            case
                when keys_on_latest_snapshot.{{ natural_key }} is not null
                    then {{ high_date }}
                else key_last_seen.last_seen_date
            end
        ) as effective_to_date
    from change_points
    left join keys_on_latest_snapshot
        on change_points.{{ natural_key }} = keys_on_latest_snapshot.{{ natural_key }}
    left join key_last_seen
        on change_points.{{ natural_key }} = key_last_seen.{{ natural_key }}
),

final as (
    select
        {{ select_list | join(',\n        ') }}
    from with_effective_dates
)

select * from final
