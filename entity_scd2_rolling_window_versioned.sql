-- Two-stage, read-time resolution over the append-only base table:
--   1. resolved: collapse corrections -- for each (entity, as-of-date), keep
--      only the latest-loaded value (highest _load_date).
--   2. the final select: collapse consecutive as-of-dates that resolve to
--      the same value into a single SCD2 version, deriving valid_from/
--      valid_to/is_current via LEAD() the same way entity_scd2_versioned
--      does for the plain daily source.
--
-- This is why entity_scd2_rolling_window doesn't try to do any of this
-- collapsing at insert time (see its header comment): resolving corrections
-- and resolving version boundaries both happen here, read-side, so a
-- correction never has to reach back and invalidate a row already inserted.

{% set attr_cols = attribute_columns(source('raw_rolling', 'system_snapshot_rolling_history')) %}
{% set all_cols = ['entity_id', '_landing_extract_date', 'attribute_hash', '_load_date', 'is_deleted'] + attr_cols %}

with resolved as (

    select {{ column_list(all_cols) }}
    from (
        select
            *,
            row_number() over (
                partition by entity_id, _landing_extract_date
                order by _load_date desc
            ) as rn
        from {{ ref('entity_scd2_rolling_window') }}
    ) ranked
    where rn = 1

),

change_flags as (

    select
        entity_id,
        _landing_extract_date,
        lag(attribute_hash) over (partition by entity_id order by _landing_extract_date) as prev_attribute_hash
    from resolved

)

select
    {{ column_list(all_cols, 'r') }},
    r._landing_extract_date as valid_from,
    lead(r._landing_extract_date) over (partition by r.entity_id order by r._landing_extract_date) as valid_to,
    case
        when lead(r._landing_extract_date) over (partition by r.entity_id order by r._landing_extract_date) is null
         and r.is_deleted = 0
        then 1 else 0
    end as is_current
from resolved r
join change_flags cf
    on r.entity_id = cf.entity_id
   and r._landing_extract_date = cf._landing_extract_date
where cf.prev_attribute_hash is null
   or r.attribute_hash <> cf.prev_attribute_hash
