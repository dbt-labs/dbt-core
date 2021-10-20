- custom macro xxxx
{% macro generate_schema_name(schema_name, node) %}

    {{ schema_name }}_{{ target.schema }}

{% endmacro %}
