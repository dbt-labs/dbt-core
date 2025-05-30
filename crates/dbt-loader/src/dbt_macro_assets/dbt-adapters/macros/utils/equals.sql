{% macro equals(expr1, expr2) %}
    {{ return(adapter.dispatch('equals', 'dbt') (expr1, expr2)) }}
{%- endmacro %}

{% macro default__equals(expr1, expr2) -%}
{%- if adapter.behavior.enable_truthy_nulls_equals_macro.no_warn %}
    case when (({{ expr1 }} = {{ expr2 }}) or ({{ expr1 }} is null and {{ expr2 }} is null))
        then 0
        else 1
    end = 0
{%- else -%}
    ({{ expr1 }} = {{ expr2 }})
{%- endif %}
{% endmacro %}
