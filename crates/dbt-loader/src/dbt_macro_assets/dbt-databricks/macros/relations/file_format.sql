{% macro file_format_clause(catalog_relation=none) %}
  {#--
    Moving forward, this macro should require a `catalog_relation`, which is covered by the first condition.
    However, there could be existing macros that is still passing no arguments, including user macros.
    Hence, we need to support the old code still, which is covered by the second condition.
  --#}
  {% if catalog_relation is not none %}
    {%- set table_format = catalog_relation.table_format -%}
    {%- set file_format = catalog_relation.file_format -%}
  {% else %}
    {%- set table_format = config.get('table_format', default='default') -%}
    {%- set file_format = adapter.resolve_file_format(config) -%}
  {% endif %}
  
  {#-- DIVERGENCE START: Instead of using a behavior flag, catalog relation should indicate whether to use managed Iceberg or UniForm --#}
  {% if table_format == 'iceberg' and catalog_relation is not none and not catalog_relation.use_uniform %}
  {#-- DIVERGENCE END #}
    using iceberg
  {% else %}
    using {{ file_format }}
  {% endif %}
{%- endmacro -%}


{% macro get_file_format(catalog_relation=none) %}
  {#-
    Moving forward, this macro should require a `catalog_relation`, which is covered by the first condition.
    However, there could be existing macros that is still passing no arguments, including user macros.
    Hence, we need to support the old code still, which is covered by the second condition.
  -#}
  {% if catalog_relation is not none %}
    {%- set raw_file_format = catalog_relation.file_format -%}
  {% else %}
    {%- set raw_file_format = adapter.resolve_file_format(config) -%}
  {% endif %}
  {% do return(dbt_databricks_validate_get_file_format(raw_file_format)) %}
{% endmacro %}
