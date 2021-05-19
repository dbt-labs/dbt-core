
{% macro default__test_relationships(model, column_name, to, field) %}

select
    child.{{ column_name }}

from {{ model }} as child

left join {{ to }} as parent
    on child.{{ column_name }} = parent.{{ field }}

where child.{{ column_name }} is not null
  and parent.{{ field }} is null

{% endmacro %}


{% test relationships(model, column_name, to, field) %}
    {% set macro = adapter.dispatch('test_relationships') %}
    {{ macro(model, column_name, to, field) }}
{% endtest %}
