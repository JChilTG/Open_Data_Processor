{{ conform_lookup_config(size='small') }}

{{ conform_lookup(
    dim='dim_gender_current',
    join_column='gender_code',
    output_columns=['gender_sk', 'gender_name'],
    mapping='map_gender',
    mapping_source_column='source_value',
    mapping_canonical_column='canonical_value',
    mapping_source_system_column='source_system',
    dedupe_order_by='d.gender_sk'
) }}
