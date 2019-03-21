
{% macro bigquery__create_csv_table(model) %}
    -- no-op
{% endmacro %}

{% macro bigquery__reset_csv_table(model, full_refresh, old_relation) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro bigquery__load_csv_rows(model) %}

  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'], model['agate_table'], column_override) }}

{% endmacro %}
