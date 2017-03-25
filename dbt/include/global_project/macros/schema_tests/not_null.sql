
{% macro test_not_null(model, arg) %}

with validation as (

    select
        {{ arg }} as not_null_field

    from {{ model }}

)

select count(*)
from validation
where not_null_field is null

{% endmacro %}

