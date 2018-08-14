
{% macro test_relationships(model, to, field) %}

{% set column_name = kwargs.get('column_name', kwargs.get('from')) %}

select count(*)
from (
    select
        model.{{ column_name }} as id
    from {{ model }} model
    left join {{ to }} target on target.{{ field }} = model.{{ column_name }}
    where
       model.{{ column_name }} is not null and
       target.{{ field }} is null
) validation_errors

{% endmacro %}
