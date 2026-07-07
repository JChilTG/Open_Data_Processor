-- Read-time view: derives valid_from/valid_to/is_current via LEAD() so the
-- base entity_scd2 table never needs an UPDATE to close out a prior version.
-- `select *` keeps this dynamic across whatever attribute columns exist.
-- A row is only "current" if it's the latest version for the entity and it
-- isn't itself a deletion marker (is_deleted only ever 1 when the
-- detect_deletions config is on for entity_scd2).

select
    *,
    _landing_extract_date as valid_from,
    lead(_landing_extract_date) over (partition by entity_id order by _landing_extract_date) as valid_to,
    case
        when lead(_landing_extract_date) over (partition by entity_id order by _landing_extract_date) is null
         and is_deleted = 0
        then 1 else 0
    end as is_current
from {{ ref('entity_scd2') }}
