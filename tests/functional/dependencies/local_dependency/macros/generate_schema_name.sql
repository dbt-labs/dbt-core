{# This should not be ignored, even as it's in a subpackage #}
{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {{ var('schema_override', target.schema) }}
{%- endmacro %}

{# This should not be ignored, even as it's in a subpackage #}
{% macro generate_database_name(custom_database_name=none, node=none) -%}
  {{ 'dbt' }}
{%- endmacro %}


{# This should not be ignored, even as it's in a subpackage #}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {{ node.name ~ '_subpackage_generate_alias_name' }}
{%- endmacro %}
