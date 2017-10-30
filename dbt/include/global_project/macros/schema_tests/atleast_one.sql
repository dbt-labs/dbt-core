{% macro test_atleast_one(model, arg) %}

with validation as (

    select
        {{ arg }} as atleast_one_field

    from {{ model }}

),

validation_errors as (


    select
      count(atleast_one_field) count_of_rows

      from validation
)

-- returns null if there is atleast one non-null value
select count(*)

  from validation_errors

  where count_of_rows = 0
{% endmacro %}