{{
  config(
    materialized='incremental',
    dist='HASH(entity_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    on_schema_change='append_new_columns',
    post_hook=[
      "alter index all on {{ this }} reorganize",
      "update statistics {{ this }}"
    ],
    detect_deletions=false
  )
}}

-- Append-only SCD2: no unique_key is set, so dbt's default incremental
-- behavior is a plain INSERT of new rows -- never MERGE/UPDATE/DELETE.
-- valid_to / is_current are derived downstream in entity_scd2_versioned via
-- LEAD(_landing_extract_date), so closing out a prior version never touches this table.
--
-- Attribute columns are never hardcoded here: `select *` picks up whatever
-- columns the source has today.
--
-- on_schema_change='append_new_columns' (not sync_all_columns): a new source
-- column gets ALTER TABLE ADD'd automatically -- safe, ordinary T-SQL. A
-- column that disappears from the source is deliberately NOT dropped: dbt's
-- generic schema-sync macros drive DROP COLUMN / ALTER COLUMN TYPE through a
-- rename-and-cascade dance that has a known open bug generating invalid T-SQL
-- against this adapter (microsoft/dbt-synapse#63), and dedicated SQL pool's
-- own DROP COLUMN support is inconsistent across versions. So a disappeared
-- column just goes NULL on new rows going forward; its history is preserved.
-- Physically dropping a stale column is a deliberate, manual decision (edit
-- the source table / dbt run --full-refresh), not an automatic daily one.
--
-- Post-hooks: dedicated SQL pool does not auto-update statistics after a
-- load (unlike Azure SQL DB), and small daily inserts land in the columnstore
-- delta store rather than a compressed row group -- both degrade query plans
-- over time if left alone, so REORGANIZE + UPDATE STATISTICS run after every
-- load (including the initial bootstrap CTAS; both are cheap/no-ops on a
-- table that doesn't need them).
--
-- is_deleted is always present so the table's schema is stable regardless of
-- the detect_deletions config; it's only ever set to 1 when that's turned on.
-- detect_deletions is a per-model config (set above), not a project var: it
-- lives in this file so each SCD2 model can turn it on/off independently.

{% if config.get('detect_deletions', false) %}
{% set attr_cols = attribute_columns(source('raw', 'system_snapshot_history')) %}
{% set all_cols = ['entity_id', '_landing_extract_date', 'attribute_hash'] + attr_cols %}
{% endif %}

with source_rows as (

    select *
    from {{ ref('stg_system_snapshot') }}

{% if is_incremental() %}
    where _landing_extract_date > (select coalesce(max(_landing_extract_date), '1900-01-01') from {{ this }})
{% endif %}

)

{% if is_incremental() %}

-- Daily run: only the newest snapshot rows are in source_rows above. Compare
-- each one to the last version already stored for that entity (found via a
-- self-join on this HASH(entity_id)-distributed table, not a full history scan).
, last_known as (

    select entity_id, attribute_hash as prev_attribute_hash, is_deleted as prev_is_deleted
    from (
        select
            entity_id,
            attribute_hash,
            is_deleted,
            row_number() over (partition by entity_id order by _landing_extract_date desc) as rn
        from {{ this }}
    ) ranked
    where rn = 1

)

{% if config.get('detect_deletions', false) %}
-- Full last-known row (not just the hash) so a deletion marker can carry
-- forward whatever attribute values the entity last had.
, last_known_full as (

    select *
    from (
        select *, row_number() over (partition by entity_id order by _landing_extract_date desc) as rn
        from {{ this }}
    ) ranked
    where rn = 1 and is_deleted = 0

)

, disappeared as (

    select
        lkf.entity_id,
        (select max(_landing_extract_date) from source_rows) as _landing_extract_date,
        cast(null as varchar(64)) as attribute_hash, -- TODO: match this cast to attribute_hash's real data type
        {{ column_list(attr_cols, 'lkf') }},
        1 as is_deleted
    from last_known_full lkf
    left join source_rows s on s.entity_id = lkf.entity_id
    where s.entity_id is null

)

select
    {{ column_list(all_cols, 's') }},
    0 as is_deleted
from source_rows s
left join last_known lk
    on s.entity_id = lk.entity_id
where lk.entity_id is null                      -- brand new entity
   or lk.prev_is_deleted = 1                     -- reappeared after being marked deleted
   or s.attribute_hash <> lk.prev_attribute_hash -- changed since last known version

union all
select * from disappeared

{% else %}

select s.*, 0 as is_deleted
from source_rows s
left join last_known lk
    on s.entity_id = lk.entity_id
where lk.entity_id is null                      -- brand new entity
   or lk.prev_is_deleted = 1                     -- reappeared after being marked deleted
   or s.attribute_hash <> lk.prev_attribute_hash -- changed since last known version

{% endif %}

{% else %}

-- One-off historical build: source_rows holds every stacked historical
-- snapshot. A single LAG pass per entity picks out only the rows where
-- something actually changed (or the entity's first appearance).
, change_flags as (

    select
        entity_id,
        _landing_extract_date,
        lag(attribute_hash) over (partition by entity_id order by _landing_extract_date) as prev_attribute_hash
    from source_rows

)

{% if config.get('detect_deletions', false) %}
-- Gap detection across the whole historical batch: for each entity's last
-- appearance before a gap, find the next date *anyone* was snapshotted
-- (calendar_next) and check whether the entity reappears there or later.
, calendar_dates as (

    select distinct _landing_extract_date from source_rows

)

, calendar_next as (

    select
        _landing_extract_date,
        lead(_landing_extract_date) over (order by _landing_extract_date) as next_calendar_date
    from calendar_dates

)

, entity_last_seen as (

    select
        *,
        lead(_landing_extract_date) over (partition by entity_id order by _landing_extract_date) as next_present_date
    from source_rows

)

, disappeared as (

    select
        els.entity_id,
        cn.next_calendar_date as _landing_extract_date,
        cast(null as varchar(64)) as attribute_hash, -- TODO: match this cast to attribute_hash's real data type
        {{ column_list(attr_cols, 'els') }},
        1 as is_deleted
    from entity_last_seen els
    join calendar_next cn on cn._landing_extract_date = els._landing_extract_date
    where cn.next_calendar_date is not null
      and (els.next_present_date is null or els.next_present_date > cn.next_calendar_date)

)

select
    {{ column_list(all_cols, 'sr') }},
    0 as is_deleted
from source_rows sr
join change_flags cf
    on sr.entity_id = cf.entity_id
   and sr._landing_extract_date = cf._landing_extract_date
where cf.prev_attribute_hash is null
   or sr.attribute_hash <> cf.prev_attribute_hash

union all
select * from disappeared

{% else %}

select sr.*, 0 as is_deleted
from source_rows sr
join change_flags cf
    on sr.entity_id = cf.entity_id
   and sr._landing_extract_date = cf._landing_extract_date
where cf.prev_attribute_hash is null
   or sr.attribute_hash <> cf.prev_attribute_hash

{% endif %}

{% endif %}
