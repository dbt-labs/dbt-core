{% macro test_call_pkg_macro(model) %}
    select {{ adapter.dispatch('current_timestamp', macro_namespace = 'local_utils')() }}
{% endmacro %}
