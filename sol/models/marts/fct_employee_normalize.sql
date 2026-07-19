{#
    Same lookup join without a persisted conform_key: normalize=true hashes
    the raw value + source system at join time. Prefer the staging-key path
    in fct_employee for production; this model shows the alternate API.
#}
{% set cj = conform_joins_ns() %}

select
    e.employee_id,
    e.raw_gender,
    {{ conform(
        cj,
        'e.raw_gender',
        'lkp_gender',
        'gender_name',
        default="'UNKNOWN'",
        normalize=true,
        source_system_column='e.source_system'
    ) }} as gender_name
from {{ ref('stg_employee') }} as e
{{ conform_joins(cj) }}
