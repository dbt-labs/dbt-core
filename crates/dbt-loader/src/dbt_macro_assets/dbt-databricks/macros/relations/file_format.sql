{% macro file_format_clause(catalog_relation=none) %}
  {% if catalog_relation is not none %}
    {%- set file_format = catalog_relation.file_format -%}
  {% else %}
    {%- set file_format = config.get('file_format', default='delta') -%}
  {% endif %}
  using {{ file_format }}
{%- endmacro -%}


{% macro get_file_format(catalog_relation=none) %}
  {% if catalog_relation is not none %}
    {%- set raw_file_format = catalog_relation.file_format -%}
  {% else %}
    {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {% endif %}
  {% do return(dbt_databricks_validate_get_file_format(raw_file_format)) %}
{% endmacro %}
