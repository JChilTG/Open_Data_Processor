{#
  SCD Type 1 account dimension for Azure Synapse Dedicated SQL Pool (T-SQL).

  SCD1 = overwrite: exactly one row per natural key, always reflecting the most
  recent snapshot. No history is kept (contrast with dim_account_scd2, which
  versions every change). Built from the same daily full-table snapshots.

  How it works:
    - Rank each account's snapshots by snapshot_date and keep the latest row
      (ROW_NUMBER + WHERE _rn = 1; Synapse has no QUALIFY).
    - Flag accounts that have dropped out of the latest extract as is_deleted = 1
      (soft delete). To make this a "current & active only" table instead, filter
      `where is_deleted = 0` downstream, or hard-delete by restricting
      current_records to accounts_on_latest_snapshot.

  Synapse-specific choices:
    - REPLICATE distribution for a small dimension (avoid data movement on joins).
    - No boolean type: is_deleted is CAST(CASE ... END AS BIT).
    - HASHBYTES surrogate key (md5 has no T-SQL equivalent).

  Graceful schema evolution (same approach as the SCD2 model):
    Attribute columns are not hardcoded. At build time we introspect the staging
    relation and pass through whatever business columns exist, so new source
    columns flow in automatically and removed ones drop out instead of raising
    "invalid column name". As a CTAS `table`, the dimension is rebuilt to match
    the current column set every run.

    Contract: `account_id` and `snapshot_date` must exist on the staging model.
    Control columns handled explicitly are in `scd1_passthrough_exclude`.
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

{#- Columns handled by the framework / not carried into the dimension. -#}
{%- set scd1_passthrough_exclude = ['attribute_hash', 'previous_attribute_hash', 'snapshot_date'] -%}
{%- set exclude_lower = scd1_passthrough_exclude | map('lower') | list -%}

{%- set passthrough_columns = [] -%}
{%- if execute -%}
    {%- set staging_columns = adapter.get_columns_in_relation(ref('stg_d365_account_snapshots')) -%}
    {%- for col in staging_columns -%}
        {%- if col.name | lower not in exclude_lower -%}
            {%- do passthrough_columns.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

with snapshots as (
    select * from {{ ref('stg_d365_account_snapshots') }}
),

latest_snapshot as (
    select max(snapshot_date) as snapshot_date
    from snapshots
),

accounts_on_latest_snapshot as (
    select distinct snapshots.account_id
    from snapshots
    inner join latest_snapshot
        on snapshots.snapshot_date = latest_snapshot.snapshot_date
),

ranked as (
    select
        *,
        row_number() over (
            partition by account_id
            order by snapshot_date desc
        ) as _rn
    from snapshots
),

current_records as (
    select *
    from ranked
    where _rn = 1
),

final as (
    select
        convert(varchar(64),
            hashbytes('SHA2_256', cast(cur.account_id as varchar(8000))), 2
        ) as account_sk,
        {% for col in passthrough_columns -%}
        cur.{{ adapter.quote(col) }},
        {% endfor -%}
        cast(
            case when latest_accounts.account_id is not null then 0 else 1 end
            as bit
        ) as is_deleted,
        cur.snapshot_date as source_snapshot_date
    from current_records as cur
    left join accounts_on_latest_snapshot as latest_accounts
        on cur.account_id = latest_accounts.account_id
)

select * from final
