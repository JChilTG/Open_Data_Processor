{% set cj = conform_joins_ns() %}

select
    e.employee_id,
    e.source_system,
    e.raw_gender,
    e.hire_date,
    {{ conform_join(cj, 'e.gender_conform_key', 'lkp_gender', 'gender_sk') }}
        as gender_sk,
    {{ conform_join(
        cj,
        'e.gender_conform_key',
        'lkp_gender',
        'gender_name',
        default="'UNKNOWN'"
    ) }} as gender_name
from {{ ref('stg_employee') }} as e
{{ conform_joins(cj) }}
