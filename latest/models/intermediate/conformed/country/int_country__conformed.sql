{{ config(materialized='view') }}

-- The conformed country dimension: every domain's marts join to this model (or, more
-- precisely, to marts/shared/dim_country built on top of it) for country attributes.
-- Nobody re-derives country cleanup logic per domain — see
-- 03-intermediate-layer-and-conformed-dimensions.md and 08-conformed-dimension-framework.md.

select
    country_iso2,
    country_iso3,
    country_name,
    region
from {{ ref('seed_country_codes') }}
