
{% test every_value_is_blue(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} != 99

{% endtest %}

