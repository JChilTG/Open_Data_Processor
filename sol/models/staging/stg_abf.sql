{#
    ABF shipments arrive with ISO alpha-2 country codes.
    Persist a source-system-scoped conform key so the fact can join on binary equality.
#}
select
    shipment_id::varchar as shipment_id,
    nullif(trim(alpha2_country), '') as alpha2_country,
    shipped_at::date as shipped_at,
    units::integer as units,
    {{ conform_key('s.alpha2_country', source_system='ABF') }}
        as country_conform_key
from {{ ref('raw_abf_shipment') }} as s
