{% macro db_api__materialized_view__refresh(relation) %}
    {{ adapter.dispatch('db_api__materialized_view__refresh', 'dbt')(relation) }}
{% endmacro %}

{% macro default__db_api__materialized_view__refresh(relation) -%}

    {{ exceptions.raise_not_implemented(
    'db_api__materialized_view__refresh not implemented for adapter '+adapter.type()) }}

{% endmacro %}
