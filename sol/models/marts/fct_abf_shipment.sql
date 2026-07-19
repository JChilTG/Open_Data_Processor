{#
    Fact keeps only the canonical country_sk. Display attributes stay on the
    dimension; both ABF (alpha2) and ABS (name) resolve through lkp_country.
#}
{% set cj = conform_joins_ns() %}

select
    a.shipment_id,
    a.shipped_at,
    a.units,
    a.alpha2_country as source_country,
    {{ conform_join(cj, 'a.country_conform_key', 'lkp_country', 'country_sk') }}
        as country_sk
from {{ ref('stg_abf') }} as a
{{ conform_joins(cj) }}
