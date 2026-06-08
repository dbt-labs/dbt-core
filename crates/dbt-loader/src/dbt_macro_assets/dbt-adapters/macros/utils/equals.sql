-- ai
-- funcsign: (string, string) -> string
{% macro equals(expr1, expr2) %}
    {{ return(adapter.dispatch('equals', 'dbt') (expr1, expr2)) }}
{%- endmacro %}

-- ai
-- funcsign: (string, string) -> string
{% macro default__equals(expr1, expr2) -%}
    {{ adapter.render_equals(expr1, expr2) }}
{%- endmacro %}
