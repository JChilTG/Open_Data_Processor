-- At most one canonical_name override per canonical code, otherwise dim_country
-- name resolution is ambiguous.
select
    upper(ltrim(rtrim(canonical_iso3))) as canonical_iso3,
    count(*) as override_count
from {{ ref('country_overrides') }}
where override_type = 'canonical_name'
group by upper(ltrim(rtrim(canonical_iso3)))
having count(*) > 1
