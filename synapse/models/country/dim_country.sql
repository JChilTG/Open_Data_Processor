{#
  Canonical country dimension for Azure Synapse Dedicated SQL Pool.

  Built from the canonical market_table (iso2, iso3, name). The display name can
  be corrected per canonical code via the country_overrides seed
  (override_type = 'canonical_name'); the override wins over the market name.
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

with market as (
    select
        upper(ltrim(rtrim(cast(iso2 as varchar(10))))) as iso2,
        upper(ltrim(rtrim(cast(iso3 as varchar(10))))) as iso3,
        ltrim(rtrim(cast(name as varchar(400)))) as name
    from {{ source('country_raw', 'market_table') }}
),

name_overrides as (
    select
        upper(ltrim(rtrim(canonical_iso3))) as iso3,
        ltrim(rtrim(canonical_name)) as canonical_name
    from {{ ref('country_overrides') }}
    where override_type = 'canonical_name'
      and canonical_name is not null
      and ltrim(rtrim(canonical_name)) <> ''
),

final as (
    select
        convert(varchar(64), hashbytes('SHA2_256', cast(m.iso3 as varchar(8000))), 2) as country_sk,
        m.iso2,
        m.iso3,
        coalesce(o.canonical_name, m.name) as name
    from market as m
    left join name_overrides as o
        on m.iso3 collate database_default = o.iso3 collate database_default
)

select * from final
