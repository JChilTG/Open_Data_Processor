{{ config(materialized='view') }}

select
    core.*,
    {{ hash_all_except(
        ref('stg_d365_account_snapshots_core'),
        var('scd2_hash_exclude_columns')
    ) }} as attribute_hash
from {{ ref('stg_d365_account_snapshots_core') }} as core
