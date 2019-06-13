
{% macro generate_schema_name(schema_name, node) -%}

    {{ schema_name }}_{{ node.schema }}_macro

{%- endmacro %}
