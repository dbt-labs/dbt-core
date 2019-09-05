
{% macro generate_schema_name(schema_name, node) %}

    {{ node.config['schema'] }}_{{ target.schema }}_macro

{% endmacro %}
