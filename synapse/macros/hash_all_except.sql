{#
  Synapse Dedicated SQL Pool version of hash_all_except, built to scale to
  hundreds of columns (tested logic for 400+).

  Why this is not a single CONCAT/HASHBYTES call:
    1. CONCAT() in a dedicated SQL pool accepts at most 254 arguments. With one
       delimiter between each column you exceed that at ~127 columns. dbt_utils'
       surrogate-key macro hits the same wall (~254). We use the `+` operator
       instead, which has no argument-count limit.
    2. HASHBYTES() in a dedicated SQL pool accepts at most 8000 bytes of input,
       and chained varchar(8000) concatenation silently TRUNCATES the result to
       8000 bytes. So we cannot just `+`-join 400 columns into one string.

  Strategy: split the columns into chunks, hash each chunk to a 64-char hex
  digest, then hash the concatenation of those digests. Each HASHBYTES input
  stays small, and new columns are still picked up automatically.

  Args:
    relation         relation whose columns are hashed
    exclude_columns  list of column names to skip (case-insensitive)
    chunk_size       columns per chunk (default 50). Lower it if individual
                     columns are very wide so each chunk stays < 8000 bytes;
                     raise it if columns are narrow.

  Usage:
    {{ hash_all_except(ref('stg_d365_account_snapshots_core'),
                       var('scd2_hash_exclude_columns')) }} as attribute_hash
#}
{% macro hash_all_except(relation, exclude_columns=[], chunk_size=50) %}
    {%- set exclude_lower = exclude_columns | map('lower') | list -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}

    {%- set col_exprs = [] -%}
    {%- for col in columns -%}
        {%- if col.name | lower not in exclude_lower -%}
            {%- do col_exprs.append(
                "coalesce(cast(" ~ adapter.quote(col.name) ~ " as varchar(8000)), '')"
            ) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if col_exprs | length == 0 -%}
        convert(varchar(64), hashbytes('SHA2_256', ''), 2)
    {%- else -%}
        {%- set chunk_hashes = [] -%}
        {%- for chunk in col_exprs | batch(chunk_size) -%}
            {%- set chunk_expr = chunk | join(" + '|' + ") -%}
            {%- do chunk_hashes.append(
                "convert(varchar(64), hashbytes('SHA2_256', " ~ chunk_expr ~ "), 2)"
            ) -%}
        {%- endfor -%}

        {%- if chunk_hashes | length == 1 -%}
            {{ chunk_hashes[0] }}
        {%- else -%}
            {#- combine the per-chunk digests: N*65 chars stays well under 8000
                for any realistic column count (>6000 cols before this matters) -#}
            convert(varchar(64), hashbytes('SHA2_256', {{ chunk_hashes | join(" + '|' + ") }}), 2)
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
