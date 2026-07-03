-- SCD1 invariant: exactly one row per natural key (overwrite, no history).
select
    account_id,
    count(*) as row_count
from {{ ref('dim_account_scd1') }}
group by account_id
having count(*) > 1
