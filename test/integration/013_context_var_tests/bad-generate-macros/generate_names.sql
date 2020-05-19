{% macro generate_schema_name(custom_schema_name, node) -%}
    {% do var('somevar') %}
    {% do return(dbt.generate_schema_name(custom_schema_name, node)) %}
{%- endmacro %}
