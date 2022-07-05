{% macro any_value(expression) -%}
    {{ return(adapter.dispatch('any_value', 'dbt') (expression)) }}
{% endmacro %}

{% macro default__any_value(expression) -%}

    any_value({{ expression }})

{%- endmacro %}
