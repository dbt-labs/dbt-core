
{% macro bigquery__create_table_as(temporary, identifier, sql) -%}
    {{ adapter.execute_model({"name": identifier, "injected_sql": sql}, 'table') }}
{% endmacro %}
