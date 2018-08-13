
{% macro test_relationships(model, to, from) %}

{% set column_name = kwargs.get('column_name', kwargs.get('field')) %}

select count(*)
from (

    select
        {{ from }} as id

    from {{ model }}
    where {{ from }} is not null
      and {{ from }} not in (
        select {{ column_name }}
        from {{ to }}
        where {{ column_name }} is not null
      )

) validation_errors

{% endmacro %}
