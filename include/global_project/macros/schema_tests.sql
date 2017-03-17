
{% macro test_not_null(model, arg) %}

with validation as (

    select
        {{ arg }} as not_null_field

    from {{ ref(model) }}

)

select count(*)
from validation
where not_null_field is null

{% endmacro %}

{% macro test_unique(model, arg) %}

with validation as (

    select
        {{ arg }} as unique_field

    from {{ ref(model) }}
    where {{ arg }} is not null

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

{% macro test_accepted_values(model, field, values) %}

with all_values as (

    select distinct
        {{ field }} as value_field

    from {{ ref(model) }}

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        {% for value in values -%}

            '{{ value }}' {% if not loop.last -%} , {%- endif %}

        {%- endfor %}
    )
)

select count(*)
from validation_errors

{% endmacro %}

{% macro test_relationships(model, from, to, field) %}

with parent as (

    select
        {{ field }} as id

    from {{ ref(model) }}

),

child as (

    select
        {{ field }} as id

    from {{ ref(to) }}

)

select count(*)
from child
where id is not null
  and id not in (select id from parent)

{% endmacro %}
