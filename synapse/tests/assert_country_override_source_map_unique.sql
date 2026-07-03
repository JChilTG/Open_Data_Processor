-- A source_map override must be unique per (source_system, match_field, value),
-- otherwise the crosswalk join can fan out. Comparison is normalized to match
-- how the crosswalk resolves values.
select
    source_system,
    match_field,
    upper(ltrim(rtrim(source_value))) as normalized_value,
    count(*) as override_count
from {{ ref('country_overrides') }}
where override_type = 'source_map'
group by source_system, match_field, upper(ltrim(rtrim(source_value)))
having count(*) > 1
