{% macro databricks__alter_column_comment(relation, column_dict) %}
  {% if config.get('file_format', default='delta') in ['delta', 'hudi'] %}
    {% for column in column_dict.values() %}
      {% set comment = column['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        alter table {{ relation.render()|lower }} change column {{ api.Column.get_name(column) }} comment '{{ escaped_comment }}';
      {% endset %}
      {% do run_query(comment_query) %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro alter_table_comment(relation, model) %}
  {% set comment_query %}
    comment on table {{ relation.render()|lower }} is '{{ model.description | replace("'", "\\'") }}'
  {% endset %}
  {% do run_query(comment_query) %}
{% endmacro %}

{% macro databricks__persist_docs(relation, model, for_relation, for_columns) -%}
  {%- if for_relation and config.persist_relation_docs() and model.description %}
    {% do alter_table_comment(relation, model) %}
  {% endif %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) -%}
    {%- set columns_to_persist_docs = adapter.get_persist_doc_columns(existing_columns, model.columns) -%}
    {% do alter_column_comment(relation, columns_to_persist_docs) %}
  {% endif %}
{% endmacro %}
