{% macro some_macro(arg1, arg2) -%}
    {{ adapter_macro('some_macro', arg1, arg2) }}
{%- endmacro %}


{% macro default__some_macro(arg1, arg2) %}
    {% do exceptions.raise_compiler_error('not allowed') %}
{% endmacro %}

{% macro postgres__some_macro(arg1, arg2) -%}
    {{ arg1 }}{{ arg2 }}
{%- endmacro %}


{% macro some_other_macro(arg1, arg2) -%}
	{{ adapter_macro('test.some_macro', arg1, arg2) }}
{%- endmacro %}
