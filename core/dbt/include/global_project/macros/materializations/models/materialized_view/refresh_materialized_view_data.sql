{% macro get_refresh_data_in_materialized_view_sql(relation) %}
    {{ adapter.dispatch('get_refresh_data_in_materialized_view_sql', 'dbt')(relation) }}
{% endmacro %}


{% macro default__get_refresh_data_in_materialized_view_sql(relation) %}
    select 1;
{% endmacro %}
