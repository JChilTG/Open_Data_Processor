-- Each account should appear at most once per extract date.
-- Valid T-SQL as-is.
select
    snapshot_date,
    account_id,
    count(*) as row_count
from {{ ref('raw_d365_account_snapshots') }}
group by snapshot_date, account_id
having count(*) > 1
