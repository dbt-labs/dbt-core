{% macro array_concat(array_1, array_2) -%}
  {{ return(adapter.dispatch('array_concat', 'dbt')(array_1, array_2)) }}
{%- endmacro %}

{% macro default__array_concat(array_1, array_2) -%}
    array_cat({{ array_1 }}, {{ array_2 }})
{%- endmacro %}
