
{% macro test_unique(model, arg) %}

select count(*)
from (

    select
        {{ arg }}

    from {{ model }}
    where {{ arg }} is not null
    group by {{ arg }}
    having count(*) > 1

) validation_errors

{% endmacro %}
