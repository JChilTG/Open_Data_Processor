select
    source_system::varchar as source_system,
    source_value::varchar as source_value,
    canonical_value::varchar as canonical_value
from {{ ref('raw_map_gender') }}
