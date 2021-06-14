{% macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp', 'test_utils')()) }}
{%- endmacro %}

{% macro default__current_timestamp() -%}
  now()
{%- endmacro %}
