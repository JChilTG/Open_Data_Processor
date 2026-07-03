{#
  Bridge: resolve Source AS country NAMEs to canonical ISO3.
  AS only provides a name (wording may differ from canonical), so it matches on
  name -- with overrides handling the wording differences.
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

{{ country_crosswalk(
    source_relation=source('country_raw', 'source_as'),
    source_system='AS',
    match_field='name',
    source_key_column='name'
) }}
