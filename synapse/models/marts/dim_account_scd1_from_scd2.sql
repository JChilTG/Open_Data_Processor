{#
  TEMPLATE: derive an SCD Type 1 (current-state) dimension from an existing
  SCD Type 2 table, for Azure Synapse Dedicated SQL Pool (T-SQL).

  Idea:
    An SCD2 table already holds one row per version with an `is_current` flag.
    Collapsing it to SCD1 is simply "keep the current version of each key and
    drop the SCD2 framework columns". This is cheaper and guaranteed consistent
    with the SCD2 history, versus rebuilding SCD1 from raw snapshots.

  Deletes:
    In the SCD2 model, a deleted account's latest version is closed on its
    last-seen date, so its `is_current` is 0. Filtering `is_current = 1` therefore
    yields only currently-active accounts -- exactly the SCD1 "current state".

  Reuse for another entity:
    Change the four values in the "Template configuration" block below
    (source relation, natural key, surrogate key name, and the SCD2 framework
    columns to drop). Everything else is column-agnostic: business columns are
    discovered at build time, so the template survives source schema changes.
#}
{{ config(
    materialized='table',
    dist='REPLICATE',
    index='HEAP'
) }}

{#- ===================== Template configuration ===================== -#}
{%- set scd2_relation = ref('dim_account_scd2') -%}
{%- set natural_key = 'account_id' -%}
{%- set surrogate_key_name = 'account_sk' -%}
{#- SCD2-only columns to drop when collapsing to SCD1 -#}
{%- set scd2_framework_columns = [
    'account_sk',
    'effective_from_date',
    'effective_to_date',
    'is_current',
] -%}
{#- ================================================================= -#}

{%- set exclude_lower = scd2_framework_columns | map('lower') | list -%}

{%- set select_list = [] -%}
{%- do select_list.append(
    "convert(varchar(64), hashbytes('SHA2_256', cast(cur." ~ natural_key
    ~ " as varchar(8000))), 2) as " ~ surrogate_key_name
) -%}
{%- if execute -%}
    {%- set scd2_columns = adapter.get_columns_in_relation(scd2_relation) -%}
    {%- for col in scd2_columns -%}
        {%- if col.name | lower not in exclude_lower -%}
            {%- do select_list.append("cur." ~ adapter.quote(col.name)) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

with current_version as (
    select *
    from {{ scd2_relation }}
    where is_current = 1
),

final as (
    select
        {{ select_list | join(',\n        ') }}
    from current_version as cur
)

select * from final
