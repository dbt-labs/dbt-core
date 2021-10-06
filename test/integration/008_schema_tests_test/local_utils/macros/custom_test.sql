{% macro test_dispatch(model) -%}
  {{ return(adapter.dispatch('test_dispatch', macro_namespace = 'local_utils')()) }}
{%- endmacro %}

{% macro default__test_dispatch(model) %}
    select {{ adapter.dispatch('current_timestamp', macro_namespace = 'local_utils')() }}
{% endmacro %}
