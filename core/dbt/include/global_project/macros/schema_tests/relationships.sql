
{% macro default__test_relationships(model, column_name, to, field) %}

with child as (
    select {{ column_name }} as from_field
    from {{ model }}
    where {{ column_name }} is not null
),

parent as (
    select {{ field }} as to_field
    from {{ to }}
)

select
    from_field

from child
left join parent
    on child.from_column = parent.to_column

where parent.to_column is null

{% endmacro %}


{% test relationships(model, column_name, to, field) %}
    {% set macro = adapter.dispatch('test_relationships') %}
    {{ macro(model, column_name, to, field) }}
{% endtest %}
