{% macro generate_schema_name(schema_name) -%}
	{%- if schema_name is none -%}
		{{ target.schema }}
	{%- else -%}
		{{ schema_name }}
	{%- endif -%}
{%- endmacro %}
