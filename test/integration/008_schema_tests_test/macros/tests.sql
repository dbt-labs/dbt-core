

{% macro test_every_value_is_blue(model, arg) %}

    select
        count(*)

    from {{ model }}
    where {{ arg }} != 'blue'

{% endmacro %}


{% macro test_rejected_values(model, field, values) %}

    select
        count(*)

    from {{ model }}
    where {{ field }} in (
        {% for value in values %}
            {{ value }} {% if not loop.last %} , {% endif %}
        {% endfor %}
    )

{% endmacro %}
