
{% macro default__test_not_null(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select *
from {{ model }}
where {{ column_name }} is null

{% endmacro %}

{% test not_null(model) %}
    {% set macro = adapter.dispatch('test_not_null') %}
    {{ macro(model, **kwargs) }}
{% endtest %}
