
{% macro test_unique(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select count(*)
from (

    select
        aliased.{{ column_name }}

    from {{ model }} AS aliased
    where aliased.{{ column_name }} is not null
    group by aliased.{{ column_name }}
    having count(*) > 1

) validation_errors

{% endmacro %}
