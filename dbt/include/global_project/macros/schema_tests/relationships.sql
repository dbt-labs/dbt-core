
{% macro test_relationships(model, field, to, from) %}


select count(*)
from (

    select
        {{ from }} as id

    from {{ model }}
    where {{ from }} is not null
      and {{ from }} not in (select {{ field }}
                             from {{ to }})

) validation_errors

{% endmacro %}
