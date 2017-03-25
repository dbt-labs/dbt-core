
{% macro test_relationships(model, field, to, from) %}

with parent as (

    select
        {{ field }} as id

    from {{ to }}

),

child as (

    select
        {{ from }} as id

    from {{ model }}

)

select count(*)
from child
where id is not null
  and id not in (select id from parent)

{% endmacro %}
