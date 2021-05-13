{% macro test_my_test(model) %}
    select {{ local_utils.current_timestamp() }}
{% endmacro %}
