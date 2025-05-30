{% macro cast(field, type) %}
  {{ return(adapter.dispatch('cast', 'dbt') (field, type)) }}
{% endmacro %}

{% macro default__cast(field, type) %}
    cast({{field}} as {{type}})
{% endmacro %}
