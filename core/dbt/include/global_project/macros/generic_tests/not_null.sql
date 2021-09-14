{% macro default__test_not_null(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null

{% endmacro %}


{% test not_null(model, column_name) %}
    {% set macro = adapter.dispatch('test_not_null', 'dbt') %}
    {{ macro(model, column_name) }}
{% endtest %}
