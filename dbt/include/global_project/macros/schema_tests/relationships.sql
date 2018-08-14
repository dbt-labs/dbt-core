
{% macro test_relationships(model, to, field) %}

{% set column_name = kwargs.get('column_name', kwargs.get('from')) %}

select count(*)
from (

    select
        {{ column_name }} as id

    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} not in (
        select {{ field }}
        from {{ to }}
        where {{ field }} is not null
      )

) validation_errors

{% endmacro %}
