
{% macro test_unique(model, arg) %}

with validation as (

    select
        {{ arg }} as unique_field

    from {{ model }}

),

validation_errors as (

    select
        unique_field

    from validation
    group by unique_field
    having count(*) > 1

)

select count(*)
from validation_errors

{% endmacro %}
