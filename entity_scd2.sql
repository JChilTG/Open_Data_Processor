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
--
-- The incremental branch seeds a LAG timeline with each entity's last known
-- state (see `timeline`/`change_flags` below) rather than comparing each new
-- row straight to that pre-batch state. This matters whenever a run ingests
-- more than one new _landing_extract_date at once (e.g. catching up after a
-- missed day): without the seeded timeline, two new dates in the same run
-- are only ever compared against the state from before the run started, so a
-- real transition between those two new dates (e.g. a value changes then
-- reverts) can be silently dropped.

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

-- Latest known row per entity -- seeds the LAG timeline below, and (when
-- detect_deletions is on) is also the carry-forward source for a deletion
-- marker's attribute values. One scan of {{ this }}, reused for both.
, last_known as (

    select *
    from (
        select *, row_number() over (partition by entity_id order by _landing_extract_date desc) as rn
        from {{ this }}
    ) ranked
    where rn = 1

)

-- Seed each entity's timeline with its last known state so a run that
-- ingests more than one new snapshot date at once (e.g. catching up after a
-- missed day) still detects every real transition between those dates --
-- not just a comparison against the state from before this run started.
-- source_rows is already filtered to dates after last_known's date, so
-- ordering the combined timeline by date is safe.
, timeline as (

    select entity_id, _landing_extract_date, attribute_hash, is_deleted, 0 as is_new
    from last_known

    union all

    select entity_id, _landing_extract_date, attribute_hash, 0 as is_deleted, 1 as is_new
    from source_rows

)

, change_flags as (

    select
        entity_id,
        _landing_extract_date,
        is_new,
        lag(attribute_hash) over (partition by entity_id order by _landing_extract_date) as prev_attribute_hash,
        lag(is_deleted) over (partition by entity_id order by _landing_extract_date) as prev_is_deleted
    from timeline

)

{% if config.get('detect_deletions', false) %}
-- Same calendar-gap approach as the bootstrap branch below, but the calendar
-- is just this run's new dates, and the timeline being checked for gaps
-- starts from the seed row -- so an entity absent from the whole batch, and
-- one that appears mid-batch then vanishes before the batch's last date, are
-- both caught the same way.
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
        entity_id,
        _landing_extract_date,
        is_deleted,
        lead(_landing_extract_date) over (partition by entity_id order by _landing_extract_date) as next_present_date
    from timeline

)

, disappeared as (

    select
        els.entity_id,
        cn.next_calendar_date as _landing_extract_date,
        cast(null as varchar(64)) as attribute_hash, -- TODO: match this cast to attribute_hash's real data type
        {% for col in attr_cols -%}
        coalesce(sr.{{ col }}, lk.{{ col }}){{ ", " if not loop.last }}
        {% endfor -%},
        1 as is_deleted
    from entity_last_seen els
    join calendar_next cn on cn._landing_extract_date = els._landing_extract_date
    left join source_rows sr
        on sr.entity_id = els.entity_id and sr._landing_extract_date = els._landing_extract_date
    left join last_known lk
        on lk.entity_id = els.entity_id
    where cn.next_calendar_date is not null
      and (els.next_present_date is null or els.next_present_date > cn.next_calendar_date)
      and els.is_deleted = 0
      and exists (select 1 from source_rows) -- belt-and-suspenders: an empty daily batch must never look like "everyone disappeared" (the calendar_next join above already guarantees this, since calendar_dates is empty when source_rows is)

)

select
    {{ column_list(all_cols, 's') }},
    0 as is_deleted
from source_rows s
join change_flags cf
    on s.entity_id = cf.entity_id
   and s._landing_extract_date = cf._landing_extract_date
where cf.is_new = 1
  and (
    cf.prev_attribute_hash is null       -- brand new entity
    or cf.prev_is_deleted = 1            -- reappeared after being marked deleted
    or s.attribute_hash <> cf.prev_attribute_hash -- changed since the previous version in this timeline
  )

union all
select * from disappeared

{% else %}

select s.*, 0 as is_deleted
from source_rows s
join change_flags cf
    on s.entity_id = cf.entity_id
   and s._landing_extract_date = cf._landing_extract_date
where cf.is_new = 1
  and (
    cf.prev_attribute_hash is null       -- brand new entity
    or cf.prev_is_deleted = 1            -- reappeared after being marked deleted
    or s.attribute_hash <> cf.prev_attribute_hash -- changed since the previous version in this timeline
  )

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
