{% macro array_append(array, new_element) -%}
  {{ return(adapter.dispatch('array_append', 'dbt')(array, new_element)) }}
{%- endmacro %}

{# new_element must be the same data type as elements in array to match postgres functionality #}
{% macro default__array_append(array, new_element) -%}
    array_append({{ array }}, {{ new_element }})
{%- endmacro %}
