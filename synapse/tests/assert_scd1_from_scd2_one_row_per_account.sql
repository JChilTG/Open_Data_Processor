-- SCD1 invariant: exactly one (current) row per natural key after collapsing SCD2.
select
    account_id,
    count(*) as row_count
from {{ ref('dim_account_scd1_from_scd2') }}
group by account_id
having count(*) > 1
