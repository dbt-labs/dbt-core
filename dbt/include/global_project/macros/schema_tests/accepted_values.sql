
{% macro test_accepted_values(model, values) %}

{% set column_name = kwargs.get('column_name', kwargs.get('field')) %}

with all_values as (

    select distinct
        {{ column_name }} as value_field

    from {{ model }}

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
