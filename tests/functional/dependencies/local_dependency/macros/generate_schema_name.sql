{# This should be ignored as it's in a subpackage #}
{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {{ exceptions.raise_compiler_error('invalid', node=node) }}
{%- endmacro %}

{# This should be ignored as it's in a subpackage #}
{% macro generate_database_name(custom_database_name=none, node=none) -%}
  {{ exceptions.raise_compiler_error('invalid', node=node) }}
{%- endmacro %}


{# This should be ignored as it's in a subpackage #}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {{ exceptions.raise_compiler_error('invalid', node=node) }}
{%- endmacro %}
