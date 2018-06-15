
{% macro test_relationships(model, field, to, from) %}

select count(*)
from (
    select
        t1.{{ from }}
    from {{ model }} t1
    left outer join {{ to }} t2
        on  t1.{{ from }} = t2.{{ field }}
    where t1.{{ from }} is not null
        and	t2.{{ field }} is null
) validation_errors

{% endmacro %}
