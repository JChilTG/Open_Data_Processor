{% macro update_statistics(columns=none, fullscan=false, sample_percent=none) %}
{#-
    Returns an `UPDATE STATISTICS` statement for the current model (`{{ this }}`).
    Meant to be used as a model's `post_hook` so a newly built/rebuilt table always
    gets fresh statistics without anyone having to remember to add one by hand — see
    05-synapse-dedicated-pool-guidance.md's "Statistics" section for why this matters
    on dedicated SQL pool (CTAS output doesn't always get statistics as aggressively as
    the query optimizer needs).

    Params:
      columns: optional list of column names to update statistics on (typically your
        join/filter/group-by keys). Omit to update statistics for the whole table —
        the safe default, and what's wired in project-wide for the marts layer (see
        dbt_project.yml).
      fullscan: scan every row instead of sampling. More accurate, more expensive —
        reach for this on a dimension small enough that the cost doesn't matter, or
        after a bulk historical load where sampled stats would badly misjudge
        cardinality.
      sample_percent: scan only this percentage of rows instead of the engine's
        default sample. A middle ground between the default and `fullscan` for large
        fact tables where accurate stats matter but a full scan is too slow to run on
        every build.

    Usage — project-wide default (whole table, every mart, see dbt_project.yml):

        models:
          your_project:
            marts:
              +post-hook: "{{ update_statistics() }}"

    Usage — per-model override, e.g. a large fact table where only the join/filter
    columns matter and a full-table scan is too expensive:

        {{ config(
            materialized='table',
            dist='HASH(customer_key)',
            as_columnstore=true,
            post_hook=update_statistics(columns=['customer_key', 'order_date'], sample_percent=25)
        ) }}

    A model-level `post_hook` runs in ADDITION to the project-level one configured in
    dbt_project.yml, not instead of it — dbt concatenates hooks rather than overriding.
    If you set a more targeted post_hook on a specific mart, remove `+post-hook` for
    that model's path (or accept that both the whole-table and targeted stats update
    will run; harmless, just extra work).
-#}
update statistics {{ this }}
{%- if columns %} ({{ columns | join(', ') }}){% endif -%}
{%- if fullscan %} with fullscan
{%- elif sample_percent %} with sample {{ sample_percent }} percent
{%- endif %}
{%- endmacro %}
