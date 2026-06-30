{#
  SCD Type 2 account dimension for Azure Synapse Dedicated SQL Pool (T-SQL).

  Synapse-specific choices vs. the DuckDB model:
    - REPLICATE distribution: this is a small dimension, so replicating it to
      every distribution avoids data movement on joins. Use ROUND_ROBIN / HASH
      for large facts instead.
    - Date math: `lead(...) - interval 1 day`  ->  DATEADD(day, -1, lead(...)).
    - High date literal: `date '9999-12-31'`     ->  CAST('9999-12-31' AS DATE).
    - No boolean type: `... as is_current`        ->  CAST(CASE ... END AS BIT).
    - Inequality `!=` works, but `<>` is the T-SQL idiom.

  Graceful schema evolution:
    The attribute columns are NOT hardcoded. At build time we introspect the
    staging relation and pass through whatever business columns exist, so:
      - a NEW source column flows into the dimension automatically (and into
        change detection, since hash_all_except is also dynamic); and
      - a REMOVED source column simply drops out instead of raising
        "invalid column name".
    Because the model is materialized as `table` (CTAS drop + recreate), the
    physical dimension is rebuilt to match the current column set every run --
    no manual ALTER TABLE is needed.

    Contract: the natural key (`account_id`), the snapshot grain
    (`snapshot_date`), and `attribute_hash` must exist on the staging model.
    These are the SCD2 framework columns; losing one is a real contract break
    and should fail loudly. `scd2_passthrough_exclude` below lists the control
    columns that are handled explicitly and therefore not passed through.
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

{#- Columns produced/handled by the SCD2 framework, excluded from passthrough. -#}
{%- set scd2_passthrough_exclude = ['attribute_hash', 'previous_attribute_hash', 'snapshot_date'] -%}
{%- set exclude_lower = scd2_passthrough_exclude | map('lower') | list -%}

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

account_last_seen as (
    select
        account_id,
        max(snapshot_date) as last_seen_date
    from snapshots
    group by account_id
),

with_previous_hash as (
    select
        *,
        lag(attribute_hash) over (
            partition by account_id
            order by snapshot_date
        ) as previous_attribute_hash
    from snapshots
),

change_points as (
    select *
    from with_previous_hash
    where
        previous_attribute_hash is null
        or attribute_hash <> previous_attribute_hash
),

with_effective_dates as (
    select
        change_points.*,
        change_points.snapshot_date as effective_from_date,
        coalesce(
            dateadd(day, -1, lead(change_points.snapshot_date) over (
                partition by change_points.account_id
                order by change_points.snapshot_date
            )),
            case
                when accounts_on_latest_snapshot.account_id is not null
                    then cast('9999-12-31' as date)
                else account_last_seen.last_seen_date
            end
        ) as effective_to_date
    from change_points
    left join accounts_on_latest_snapshot
        on change_points.account_id = accounts_on_latest_snapshot.account_id
    left join account_last_seen
        on change_points.account_id = account_last_seen.account_id
),

final as (
    select
        convert(varchar(64),
            hashbytes('SHA2_256',
                concat(account_id, '|', cast(effective_from_date as varchar(10)))
            ), 2) as account_sk,
        {% for col in passthrough_columns -%}
        {{ adapter.quote(col) }},
        {% endfor -%}
        effective_from_date,
        effective_to_date,
        cast(
            case when effective_to_date = cast('9999-12-31' as date) then 1 else 0 end
            as bit
        ) as is_current,
        snapshot_date as source_snapshot_date
    from with_effective_dates
)

select * from final
