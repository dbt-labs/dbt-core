

{% macro test_every_value_is_blue(model, column_name) %}

    select
        count(*)

    from {{ model }}
    where {{ column_name }} != 'blue'

{% endmacro %}


{% macro test_rejected_values(model, column_name, values) %}

    select
        count(*)

    from {{ model }}
    where {{ column_name }} in (
        {% for value in values %}
            '{{ value }}' {% if not loop.last %} , {% endif %}
        {% endfor %}
    )

{% endmacro %}


{% macro test_equivalent(model, value) %}
    {% set expected = 'foo-bar' %}
    select
    {% if value == expected %}
        0
    {% else %}
        {% set msg -%}
        got "{{ value }}", expected "{{ expected }}"
        {%- endset %}
        {% do log(msg, info=True) %}
        1
    {% endif %}
    as id
{% endmacro %}
