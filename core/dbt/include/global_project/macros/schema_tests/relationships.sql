
{% macro default__test_relationships(model, column_name, to, field) %}

with child as (
    select * from {{ model }}
    where {{ column_name }} is not null
),

parent as (
    select * from {{ to }}
)

select
    child.{{ column_name }}

from child
left join parent
    on child.{{ column_name }} = parent.{{ field }}

where parent.{{ field }} is null

{% endmacro %}


{% test relationships(model, column_name, to, field) %}
    {% set macro = adapter.dispatch('test_relationships') %}
    {{ macro(model, column_name, to, field) }}
{% endtest %}
