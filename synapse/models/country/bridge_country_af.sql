{#
  Bridge: resolve Source AF ISO2 codes to canonical ISO3.
  AF provides an iso2 that may differ from canonical (e.g. UK->GB, EL->GR), so it
  matches on iso2 -- with overrides handling the differences.
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

{{ country_crosswalk(
    source_relation=source('country_raw', 'source_af'),
    source_system='AF',
    match_field='iso2',
    source_key_column='iso2'
) }}
