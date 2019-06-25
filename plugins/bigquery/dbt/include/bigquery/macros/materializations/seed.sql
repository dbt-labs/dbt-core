
{% macro bigquery__create_csv_table(model, csv_table) %}
    -- no-op
{% endmacro %}

{% macro bigquery__reset_csv_table(model, full_refresh, old_relation, csv_table) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro bigquery__load_csv_rows(model, csv_table) %}

  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'], csv_table, column_override) }}

{% endmacro %}
