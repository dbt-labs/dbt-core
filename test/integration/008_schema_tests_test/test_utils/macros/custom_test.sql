{% macro test_dispatch(model) -%}
  {{ return(adapter.dispatch('test_dispatch', macro_namespace = 'test_utils')()) }}
{%- endmacro %}

{% macro default__test_dispatch(model) %}
    select {{ adapter.dispatch('current_timestamp', macro_namespace = 'test_utils')() }}
{% endmacro %}
