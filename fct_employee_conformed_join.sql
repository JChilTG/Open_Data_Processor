-- Exercise Synapse-safe conform_join (LEFT JOINs, not correlated subqueries)
{% set joins = conform_joins_ns() %}

select
    e.employee_id,

    {{ conform_join(joins, 'e.gender_code', 'dim_gender', 'gender_code') }} as gender_sk,

    {{ conform_join(joins, 'e.gender_code', 'dim_gender', 'gender_code',
                    output_column='gender_name') }} as gender_name,

    {{ conform_join(joins, 'e.raw_gender', 'dim_gender', 'gender_code',
                    mapping='map_gender') }} as gender_sk_mapped,

    {{ conform_join(joins, 'e.raw_gender', 'dim_gender', 'gender_code',
                    mapping='map_gender',
                    source_system='hr_system') }} as gender_sk_hr,

    {{ conform_join(joins, 'e.raw_gender', 'dim_gender', 'gender_code',
                    mapping='map_gender',
                    source_system='legacy') }} as gender_sk_legacy

from {{ ref('stg_employee') }} e
{{ conform_joins(joins) }}
