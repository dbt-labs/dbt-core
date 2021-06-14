{% macro test_call_pkg_macro(model) %}
    select {{ local_utils.current_timestamp() }}
{% endmacro %}
