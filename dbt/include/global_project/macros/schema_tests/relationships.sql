
{% macro test_relationships(model, field, to, from) %}

select count(*)
from (

    select distinct
        m.{{ from }} as id
    from {{ model }} m
    left join {{ to }} t on t.{{ field }} = m.{{ from }}
    where
       m.{{ from }} is not null and
       t.{{ field }} is null

) validation_errors

{% endmacro %}
