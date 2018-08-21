
{% macro test_relationships(model, to, field) %}

{% set column_name = kwargs.get('column_name', kwargs.get('from')) %}


select count(*)
from (
    select {{ column_name }} as id from {{ model }}
) as child
left join (
    select {{ field }} as id from {{ to }}
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null

{% endmacro %}

