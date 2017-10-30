
{% macro test_not_constant(model, arg) %}

with validation as (

    select
        {{ arg }} as not_constant_field

    from {{ model }}
    where {{ arg }} is not null

),

validation_errors as (

    select
        count(distinct not_constant_field) number_of_distinct_rows

    from validation

)

select count(*)
from validation_errors
  where number_of_distinct_rows = 1

{% endmacro %}
