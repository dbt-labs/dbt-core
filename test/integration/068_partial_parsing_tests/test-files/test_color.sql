{% test check_color(model, column_name, color) %}

    select *
    from {{ model }}
    where {{ column_name }} = '{{ color }}'

{% endtest %}
