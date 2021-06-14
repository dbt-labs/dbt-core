{% test every_value_is_blue(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} != 'blue'

{% endtest %}


{% test rejected_values(model, column_name, values) %}

    select *
    from {{ model }}
    where {{ column_name }} in (
        {% for value in values %}
            '{{ value }}' {% if not loop.last %} , {% endif %}
        {% endfor %}
    )

{% endtest %}


{% test equivalent(model, value) %}
    {% set expected = 'foo-bar' %}
    {% set eq = 1 if value == expected else 0 %}
    {% set validation_message -%}
      'got "{{ value }}", expected "{{ expected }}"'
    {%- endset %}
    {% if eq == 0 and execute %}
        {{ log(validation_message, info=True) }}
    {% endif %}

    select {{ validation_message }} as validation_error
    where {{ eq }} = 0
{% endtest %}

