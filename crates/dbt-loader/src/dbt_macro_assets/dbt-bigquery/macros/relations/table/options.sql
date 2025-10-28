{% macro bigquery_table_options(config, node, temporary, catalog_relation) %}
  {% set opts = adapter.get_table_options(config, node, temporary) %}
  {%- do return(bigquery_options(opts)) -%}
{%- endmacro -%}