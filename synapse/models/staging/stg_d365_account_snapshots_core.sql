{{ config(materialized='view') }}

with source as (
    select * from {{ ref('raw_d365_account_snapshots') }}
)

select
    cast(snapshot_date as date) as snapshot_date,
    account_id,
    trim(name) as name,
    trim(account_number) as account_number,
    trim(telephone) as telephone,
    trim(address_city) as address_city,
    trim(address_state) as address_state,
    trim(address_country) as address_country,
    trim(industry) as industry,
    cast(revenue as decimal(18, 2)) as revenue,
    cast(statecode as int) as statecode,
    case cast(statecode as int)
        when 0 then 'Active'
        when 1 then 'Inactive'
        else 'Unknown'
    end as account_status,
    cast(modifiedon as datetime2(0)) as modified_at
from source
