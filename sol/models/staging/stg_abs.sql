{#
    ABS orders arrive with free-text country names.
    Same conform_key pattern as ABF, scoped to source_system='ABS'.
#}
select
    order_id::varchar as order_id,
    nullif(trim(country_name), '') as country_name,
    ordered_at::date as ordered_at,
    amount::decimal(18, 2) as amount,
    {{ conform_key('s.country_name', source_system='ABS') }}
        as country_conform_key
from {{ ref('raw_abs_order') }} as s
