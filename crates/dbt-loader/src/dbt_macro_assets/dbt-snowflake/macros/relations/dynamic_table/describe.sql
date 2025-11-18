{% macro snowflake__describe_dynamic_table(relation) %}
    {%- set results = adapter.describe_dynamic_table(relation) -%}
    {%- do return(results) -%}
{% endmacro %}
