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
    detect_deletions=true
  )
}}

-- SCD2 variant for a *rolling-window* source: each delivery (assumed
-- monthly) resends full snapshots for the trailing several months, not just
-- the newest one. That means, unlike entity_scd2, the same
-- _landing_extract_date (business as-of date) can be redelivered across
-- multiple _load_date batches, and can legitimately carry a DIFFERENT value
-- than an earlier delivery reported (a correction/restatement) -- not just a
-- new value for a brand new date.
--
-- Storage philosophy: this table stores every distinct (entity_id,
-- _landing_extract_date, attribute_hash) fact ever reported, deduped only
-- when an exact match is already on file. It does NOT try to collapse
-- consecutive unchanged as-of-dates into a single version row at insert
-- time the way entity_scd2's bootstrap LAG pass does -- that collapsing, and
-- resolving which value is authoritative when a later delivery corrects an
-- earlier one, happens read-side in entity_scd2_rolling_window_versioned.
-- Pushing that logic into the view means a correction never has to reach
-- back and rewrite/invalidate a row this table already inserted: the base
-- table only ever grows by insertion, exactly like entity_scd2.
--
-- The trade-off is a wider base table: an entity whose value never changes
-- still gets one physical row per as-of-date the first time each date is
-- delivered (up to ~7 rows on its first appearance, one per month in the
-- window), not one row total. Once an as-of-date is already on file,
-- redelivering the same value for it is a no-op (see the NOT EXISTS filter
-- below) -- the extra rows are a one-time cost per entity's first
-- appearance in the window, not a per-month accumulation.
--
-- Deletion detection assumes the *newest* as-of-date(s) in a delivery are a
-- full census of every currently active entity -- that's what makes "gone
-- from the newest month" a meaningful deletion signal at all. The rest of
-- the window exists purely to allow corrections to already-recorded
-- history, and never feeds this check. An entity's data aging out of the
-- window (its last recorded as-of-date now falls further back than the
-- window reaches) is therefore never mistaken for a deletion: it simply
-- stops being mentioned, frozen at whatever was last on file.
--
-- Known scope limit: deletion detection only runs on ongoing (incremental)
-- loads, comparing each new delivery's census against what's already on
-- file. The very first (bootstrap) load has no prior delivery to compare
-- against, so it cannot retroactively reconstruct deletions that happened
-- somewhere in the middle of your accumulated historical loads. If that
-- matters for your source, it needs additional logic not included here.
--
-- min() is used below to collapse duplicate attribute values within a
-- group -- fine for ordinary scalar types, but MIN()/MAX() aren't valid on
-- some LOB/binary types in T-SQL. If any attribute column is one of those,
-- swap its min(...) for another "pick one" mechanism (e.g. a windowed
-- FIRST_VALUE).

{% set attr_cols = attribute_columns(source('raw_rolling', 'system_snapshot_rolling_history')) %}
{% set all_cols = ['entity_id', '_landing_extract_date', 'attribute_hash'] + attr_cols %}

with source_rows as (

    select *
    from {{ ref('stg_system_snapshot_rolling') }}

{% if is_incremental() %}
    where _load_date > (select coalesce(max(_load_date), '1900-01-01') from {{ this }})
{% endif %}

)

-- Collapse exact duplicate (entity, as-of-date, hash) facts to one row,
-- keeping the earliest _load_date they were reported in -- "when did we
-- first learn this," not "how many times has it been re-confirmed since."
-- This matters most on the very first run, where source_rows can span every
-- historical monthly delivery ever received at once.
, deduped as (

    select
        entity_id,
        _landing_extract_date,
        attribute_hash,
        min(_load_date) as _load_date
        {% for col in attr_cols -%}
        , min({{ col }}) as {{ col }}
        {% endfor %}
    from source_rows
    group by entity_id, _landing_extract_date, attribute_hash

)

{% if is_incremental() and config.get('detect_deletions', true) %}
-- previously_known: latest on-file state per entity, used both to find the
-- overall previous "newest as-of-date" (to identify genuinely new
-- leading-edge dates below) and to carry forward attribute values onto a
-- deletion marker.
, previously_known as (

    select *
    from (
        select *, row_number() over (partition by entity_id order by _landing_extract_date desc, _load_date desc) as rn
        from {{ this }}
    ) ranked
    where rn = 1

)

-- Genuinely new leading-edge dates: as-of-dates in this delivery that are
-- newer than anything already on file. Handles catching up after a missed
-- monthly delivery the same way entity_scd2 does -- more than one new
-- leading-edge date can appear in a single run -- without ever touching the
-- older, correction-only dates in this same delivery.
, new_leading_edge_dates as (

    select distinct _landing_extract_date
    from source_rows
    where _landing_extract_date > (select coalesce(max(_landing_extract_date), '1900-01-01') from previously_known)

)

, calendar_next as (

    select
        _landing_extract_date,
        lead(_landing_extract_date) over (order by _landing_extract_date) as next_calendar_date
    from new_leading_edge_dates

)

, entity_last_seen as (

    select
        entity_id,
        _landing_extract_date,
        lead(_landing_extract_date) over (partition by entity_id order by _landing_extract_date) as next_present_date
    from (
        select entity_id, _landing_extract_date
        from previously_known
        where is_deleted = 0

        union all

        select entity_id, _landing_extract_date
        from source_rows
        where _landing_extract_date in (select _landing_extract_date from new_leading_edge_dates)
    ) combined

)

, disappeared as (

    select
        els.entity_id,
        cn.next_calendar_date as _landing_extract_date,
        cast(null as varchar(64)) as attribute_hash, -- TODO: match this cast to attribute_hash's real data type
        {% for col in attr_cols -%}
        pk.{{ col }}{{ ", " if not loop.last }}
        {% endfor -%},
        (select max(_load_date) from source_rows) as _load_date,
        1 as is_deleted
    from entity_last_seen els
    join calendar_next cn on cn._landing_extract_date = els._landing_extract_date
    join previously_known pk on pk.entity_id = els.entity_id
    where cn.next_calendar_date is not null
      and (els.next_present_date is null or els.next_present_date > cn.next_calendar_date)
      and pk.is_deleted = 0
      and exists (select 1 from source_rows) -- belt-and-suspenders: an empty delivery must never look like "everyone disappeared"

)
{% endif %}

select
    {{ column_list(all_cols, 'd') }},
    d._load_date,
    0 as is_deleted
from deduped d

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.entity_id = d.entity_id
      and t._landing_extract_date = d._landing_extract_date
      and t.attribute_hash = d.attribute_hash
)
{% endif %}

{% if is_incremental() and config.get('detect_deletions', true) %}
union all
select * from disappeared
{% endif %}
