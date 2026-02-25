{% macro databricks__create_view_as(relation, sql) %}
  {% if column_mask_exists() %}
    {% do exceptions.raise_compiler_error("Column masks are not supported for views.") %}
  {% endif %}
  {{ log("Creating view " ~ relation) }}
  create or replace view {{ relation.render() }}
  {%- if config.persist_column_docs() -%}
    {%- set model_columns = model.columns -%}
    {%- set query_columns = get_columns_in_query(sql) -%}
    {%- if query_columns %}
  (
    {{ get_persist_docs_column_list(model_columns, query_columns) }}
  )
    {%- endif -%}
  {%- endif %}
  {{ comment_clause() }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config and contract_config.enforced %}
  {{ get_assert_columns_equivalent(sql) }}
  {%- endif -%}
  {{ tblproperties_clause() }}
  as (
    {{ sql }}
  )
{% endmacro %}


{% macro get_column_comment_sql(column_name, column_dict) -%}
  {%- if column_name in column_dict and column_dict[column_name]["description"] -%}
    {%- set escaped_description = column_dict[column_name]["description"] | replace("'", "\\'") -%}
    {%- set column_comment_clause = "comment '" ~ escaped_description ~ "'" -%}
    {{ adapter.quote(column_name) }} {{ column_comment_clause }}
  {%- else -%}
    {{ adapter.quote(column_name) }}
  {%- endif -%}
{%- endmacro %}

{% macro get_persist_docs_column_list(model_columns, query_columns) -%}
  {%- for column_name in query_columns -%}
    {{ get_column_comment_sql(column_name, model_columns) }}{{",\n\t" if not loop.last else "" }}
  {%- endfor -%}
{% endmacro %}