
{% macro generate_schema_name(schema_name) -%}

    {{ schema_name }}_{{ target.schema }}_macro

{%- endmacro %}
