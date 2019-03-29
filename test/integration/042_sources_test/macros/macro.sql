{% macro override_me() -%}
	{{ exceptions.raise_compiler_error('this is a bad macro') }}
{%- endmacro %}

{% macro happy_little_macro() -%}
	{{ override_me() }}
{%- endmacro %}
