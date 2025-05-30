{% macro file_format_clause() %}
  {%- set file_format = config.get('file_format', default='delta') -%}
  using {{ file_format }}
{%- endmacro -%}
