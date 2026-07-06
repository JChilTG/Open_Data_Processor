{% macro normalize_raw_value(column_expression) %}
{#-
    The single, shared definition of "how do we make a raw value comparable for
    conforming purposes": cast to a fixed-length varchar, trim, uppercase, and pin the
    collation to the database default.

    The collation pin matters specifically on Synapse dedicated SQL pool: a resolver
    model joins a bronze/staging column (whatever collation the source loader gave it —
    PolyBase/COPY INTO often don't match the database's own collation) against dbt seed
    data (which gets the database default). Compared directly, that raises "Cannot
    resolve the collation conflict." Fixing it here costs nothing extra, since every
    value is already being cast/trimmed/uppercased per row at this exact point.

    Every candidate source inside conform_dimension_mapping() uses this, and every
    resolver model's own raw column must use it too (see that macro's usage example) —
    using this shared definition on both sides of the eventual join, rather than each
    model rolling its own upper/trim call, is what guarantees the join key always means
    exactly the same thing everywhere and never silently diverges (e.g. one model
    trimming but forgetting to uppercase) or fails on a collation conflict.
-#}
upper(trim(cast({{ column_expression }} as varchar(255)))) collate database_default
{%- endmacro %}
