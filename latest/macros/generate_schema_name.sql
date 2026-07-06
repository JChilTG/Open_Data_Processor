{% macro generate_schema_name(custom_schema_name, node) -%}
{#-
    Overrides dbt's built-in generate_schema_name macro (this exact name is what dbt
    looks for — see https://docs.getdbt.com/docs/build/custom-schemas). Without this,
    dbt's default behavior always concatenates target.schema + custom schema
    (`<target_schema>_<custom_schema>`) regardless of environment, which means prod
    tables end up named like `analytics_marts_finance` instead of the clean
    `marts_finance` most teams actually want in production.

    Behavior:
      - No custom schema configured on the model -> use the target's schema as-is.
      - Custom schema configured, target IS the configured prod target -> use the
        custom schema name exactly as given (clean names in prod: `staging`,
        `intermediate`, `marts_finance`, ...).
      - Custom schema configured, target is anything else (dev, CI, a personal
        sandbox) -> prefix with the target's own schema so every developer's runs land
        in their own isolated schema and never collide with prod or each other
        (e.g. `dbt_jdoe_marts_finance`).

    Which target counts as "prod" is read from the `prod_target_name` var (default
    'prod') rather than hardcoded, since target naming conventions vary by project —
    override it in dbt_project.yml if your profile's production target is named
    something else (e.g. 'production').

    This only takes effect for models that set a `schema:` config (see
    dbt_project.yml's `+schema` entries per layer) — a model with no custom schema
    config always just builds in target.schema, in every environment.
-#}
    {%- set default_schema = target.schema -%}
    {%- set prod_target_name = var('prod_target_name', 'prod') -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name == prod_target_name -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
