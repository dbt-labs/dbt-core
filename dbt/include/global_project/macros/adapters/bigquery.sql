
{% macro bigquery__create_table_as(temporary, identifier, sql) -%}
    {{ adapter.execute_model({"name": identifier, "injected_sql": sql, "schema": schema}, 'table') }}
{% endmacro %}

{% macro bigquery__create_view_as(identifier, sql) -%}
    {{ adapter.execute_model({"name": identifier, "injected_sql": sql, "schema": schema}, 'view') }}
{% endmacro %}
