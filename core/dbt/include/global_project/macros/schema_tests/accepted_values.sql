{% macro default__test_accepted_values(model, column_name, values, quote=True) %}

with all_values as (

    select distinct
        {{ column_name }} as value_field

    from {{ model }}

)

select
    value_field

from all_values
where value_field not in (
    {% for value in values -%}
        {% if quote -%}
        '{{ value }}'
        {%- else -%}
        {{ value }}
        {%- endif -%}
        {%- if not loop.last -%},{%- endif %}
    {%- endfor %}
)

{% endmacro %}


{% test accepted_values(model, column_name, values, quote=True) %}
    {% set macro = adapter.dispatch('test_accepted_values') %}
    {{ macro(model, column_name, values, quote) }}
{% endtest %}
