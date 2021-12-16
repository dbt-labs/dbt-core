{% macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp')) }}
{%- endmacro %}

{% macro default__current_timestamp() -%}
  now()
{%- endmacro %}
