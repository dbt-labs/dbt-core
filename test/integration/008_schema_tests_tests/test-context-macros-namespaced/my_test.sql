{% macro test_call_pkg_macro(model) %}
    select {{ test_utils.current_timestamp() }}
{% endmacro %}
