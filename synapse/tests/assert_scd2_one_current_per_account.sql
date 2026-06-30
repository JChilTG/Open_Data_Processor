-- SCD2: at most one open (current) row per natural key.
-- Synapse: is_current is a BIT, so it must be compared with = 1 (no implicit boolean).
select
    account_id,
    count(*) as current_row_count
from {{ ref('dim_account_scd2') }}
where is_current = 1
group by account_id
having count(*) > 1
