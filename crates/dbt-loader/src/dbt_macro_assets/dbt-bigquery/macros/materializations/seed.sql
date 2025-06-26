
{% macro bigquery__create_csv_table(model, agate_table) %}
    -- no-op
{% endmacro %}

{% macro bigquery__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro bigquery__load_csv_rows(model, agate_table) %}

  {# -- already layered into the schema of the agate table upstream #}
  {%- set table_after_schema_override = agate_table -%}

  {%- set delimiter = model['config'].get('delimiter', ',') -%}
  {{ adapter.load_dataframe(
      model['database'],
      model['schema'],
      model['alias'],
      model['project_root'] | string ~ model['original_file_path'] | string,
      agate_table,
      delimiter,
  ) }}

  {% call statement() %}
    alter table {{ this.render() }} set {{ bigquery_table_options(config, model) }}
  {% endcall %}

  {% if config.persist_relation_docs() and 'description' in model %}

  	{{ adapter.update_table_description(model['database'], model['schema'], model['alias'], model['description']) }}
  {% endif %}
{% endmacro %}
