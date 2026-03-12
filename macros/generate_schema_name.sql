-- Macro que respeta el schema custom tal cual, sin prepend del target schema.
-- Así models/silver → schema "silver", models/gold → schema "gold".
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
