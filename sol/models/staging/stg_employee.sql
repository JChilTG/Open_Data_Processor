select
    employee_id::integer as employee_id,
    source_system::varchar as source_system,
    nullif(trim(raw_gender), '') as raw_gender,
    hire_date::date as hire_date,
    {{ conform_key('s.raw_gender', source_system_column='s.source_system') }}
        as gender_conform_key
from {{ ref('raw_employee') }} as s
