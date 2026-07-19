-- Synapse dedicated SQL pool: REPLICATE small dims so joins broadcast (no shuffle).
-- Ignored harmlessly by dbt-duckdb when unsupported.
{{ config(dist='replicate') }}

select *
from (
    values
        (1, 'M', 'Male'),
        (2, 'F', 'Female'),
        (3, 'X', 'Non-binary')
) as t(gender_sk, gender_code, gender_name)
