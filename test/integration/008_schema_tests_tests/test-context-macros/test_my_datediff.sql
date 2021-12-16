{% macro test_my_datediff(model) %}
    select {{ local_utils.datediff() }}
{% endmacro %}
