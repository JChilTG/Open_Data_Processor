-- SCD2: effective date ranges must not overlap for the same account.
-- This is valid T-SQL as-is (no boolean column referenced).
with ordered as (
    select
        account_id,
        effective_from_date,
        effective_to_date,
        lead(effective_from_date) over (
            partition by account_id
            order by effective_from_date
        ) as next_effective_from_date
    from {{ ref('dim_account_scd2') }}
)

select *
from ordered
where
    next_effective_from_date is not null
    and effective_to_date >= next_effective_from_date
