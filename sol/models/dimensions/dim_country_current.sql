select
    country_sk::integer as country_sk,
    alpha2::varchar as alpha2,
    country_name::varchar as country_name
from {{ ref('raw_dim_country') }}
