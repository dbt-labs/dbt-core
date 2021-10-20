{% test is_odd(model, column_name) %}

with validation as (

    select
        {{ column_name }} as odd_field

    from {{ model }}

),

validation_errors as (

    select
        odd_field

    from validation
    -- if this is true, then odd_field is actually even!
    where (odd_field % 2) = 0

)

select *
from validation_errors

{% endtest %}