-- Exercise conform: direct match (auto _sk), named output, mapping seed, source_system filter
select
    employee_id,

    {{ conform('gender_code', 'dim_gender', 'gender_code') }} as gender_sk,

    {{ conform('gender_code', 'dim_gender', 'gender_code',
               output_column='gender_name') }} as gender_name,

    {{ conform('raw_gender', 'dim_gender', 'gender_code',
               mapping='map_gender') }} as gender_sk_mapped,

    {{ conform('raw_gender', 'dim_gender', 'gender_code',
               mapping='map_gender',
               source_system='hr_system') }} as gender_sk_hr,

    {{ conform('raw_gender', 'dim_gender', 'gender_code',
               mapping='map_gender',
               source_system='legacy') }} as gender_sk_legacy

from {{ ref('stg_employee') }}
