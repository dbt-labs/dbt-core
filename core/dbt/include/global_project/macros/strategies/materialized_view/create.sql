{% macro strategy__materialized_view__create(relation, sql) %}
    {{ adapter.dispatch('strategy__materialized_view__create', 'dbt')(relation, sql) }}
{% endmacro %}

{% macro default__strategy__materialized_view__create(relation, sql) %}
    {{ db_api__materialized_view__create(relation, sql) }}
{% endmacro %}
