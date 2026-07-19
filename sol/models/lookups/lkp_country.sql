{{ conform_lookup_config(size='small') }}

{{ conform_lookup(
    dim='dim_country_current',
    join_column='alpha2',
    output_columns=['country_sk', 'alpha2', 'country_name'],
    mapping='map_country',
    mapping_source_column='source_value',
    mapping_canonical_column='canonical_value',
    mapping_source_system_column='source_system',
    dedupe_order_by='d.country_sk'
) }}
