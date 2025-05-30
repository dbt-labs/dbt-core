{% macro databricks__create_view_as(relation, sql) -%}
  create or replace view {{ relation.render() }}
  {% if config.persist_column_docs() -%}
    {% set model_columns = model.columns %}
    {% set query_columns = get_columns_in_query(sql) %}
    {% if query_columns %}
      (
        {{ get_persist_docs_column_list(model_columns, query_columns) }}
      )
    {% endif %}
  {% endif %}
  {{ comment_clause() }}
  {%- set contract_config = config.get('contract') -%}
  {% if contract_config and contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  {{ tblproperties_clause() }}
  as
    {{ sql }}
{% endmacro %}
