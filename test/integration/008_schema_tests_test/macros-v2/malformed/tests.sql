-- tries to build a macro that always returns an empty set
{% macro test_empty_aggregation(model, column_name) %}

SELECT * from {{ model }} WHERE 1=0

{% endmacro %}
