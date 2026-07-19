{#
    Same lookup and country_sk grain as fct_abf_shipment, despite ABS sending
    country names instead of alpha-2 codes.
#}
{% set cj = conform_joins_ns() %}

select
    a.order_id,
    a.ordered_at,
    a.amount,
    a.country_name as source_country,
    {{ conform_join(cj, 'a.country_conform_key', 'lkp_country', 'country_sk') }}
        as country_sk
from {{ ref('stg_abs') }} as a
{{ conform_joins(cj) }}
