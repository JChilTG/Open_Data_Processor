{{ config(materialized='view') }}

-- WORKED EXAMPLE, not a real source. Demonstrates the per-source resolver pattern
-- end-to-end against seeds/conformed_dimensions/country/demo/seed_demo_source_country_raw.csv
-- so the framework can be built and inspected on a dev/UAT Synapse dedicated SQL pool
-- without any real bronze data (this project's SQL is Synapse-specific — see
-- 08-conformed-dimension-framework.md — so it won't build against a generic local
-- database).
--
-- To onboard a REAL source, copy this file's shape into
-- models/intermediate/conformed/country/, swap `source_rows` to select from your
-- `stg_<source>__<entity>` model's raw country column, and change `source_system` to
-- your source's name (only needed if that source requires its own overrides).

with source_rows as (

    select
        demo_record_id,
        {{ normalize_raw_value('country_raw') }} as raw_value
    from {{ ref('seed_demo_source_country_raw') }}

),

value_map as (

    {{ conform_dimension_mapping(dimension='country', source_system='demo_source') }}

),

resolved as (

    select
        source_rows.demo_record_id,
        coalesce(value_map.country_iso3, {{ dimension_unknown_value('country') }}) as country_iso3
    from source_rows
    left join value_map
        on source_rows.raw_value = value_map.raw_value

)

select * from resolved
