select
    gender_sk::integer as gender_sk,
    gender_code::varchar as gender_code,
    gender_name::varchar as gender_name
from {{ ref('raw_dim_gender') }}
