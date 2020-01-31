{% macro get_catalog(information_schema, schemas) %}
	{% do exceptions.raise_compiler_error('rejected: no catalogs for you') %}
{% endmacro %}
