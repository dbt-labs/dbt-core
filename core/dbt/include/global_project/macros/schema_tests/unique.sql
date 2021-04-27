
{% macro default__test_unique(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select *
from (

    select
        {{ column_name }}

    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) > 1

) validation_errors

{% endmacro %}


{% test unique(model) %}
    {% set macro = adapter.dispatch('test_unique') %}
    {{ macro(model, **kwargs) }}
{% endtest %}
