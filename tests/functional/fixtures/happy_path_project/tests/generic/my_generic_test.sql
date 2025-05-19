{% test my_generic_test(model, column_name) %}
    SELECT 1 as a_column WHERE a_column = 2
{% endtest %}
