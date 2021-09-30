{% macro some_macro(arg1, arg2) -%}
    {{ adapter_macro('some_macro', arg1, arg2) }}
{%- endmacro %}
