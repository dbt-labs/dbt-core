{% macro refresh_materialized_view(relation) %}
    {{ adapter.dispatch('refresh_materialized_view', 'dbt')(relation) }}
{% endmacro %}


{% macro default__refresh_materialized_view(relation) %}
    select 1;
{% endmacro %}
