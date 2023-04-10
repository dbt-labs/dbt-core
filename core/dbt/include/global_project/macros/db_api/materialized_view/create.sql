{% macro db_api__materialized_view__create(relation, sql) %}
    {{ adapter.dispatch('db_api__materialized_view__create', 'dbt')(relation, sql) }}
{% endmacro %}

{% macro default__db_api__materialized_view__create(relation, sql) -%}

    {{ exceptions.raise_not_implemented(
    'db_api__materialized_view__create macro not implemented for adapter '+adapter.type()) }}

{% endmacro %}
