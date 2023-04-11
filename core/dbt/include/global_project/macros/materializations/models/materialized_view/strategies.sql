{% macro materialized_view__get_create_view_as_sql(relation, sql) -%}
  {{ adapter.dispatch('materialized_view__get_create_view_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro default__get_create_view_as_sql(relation, sql) -%}
  {{ return(materialized_view__create_view_as(relation, sql)) }}
{% endmacro %}


/* {# keep logic under old name for backwards compatibility #} */
{% macro materialized_view__create_view_as(relation, sql) -%}
  {{ adapter.dispatch('create_view_as', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro default__materialized_view__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro materialized_view__full_refresh(relation, sql, backup_relation=None, intermediate_relation=None) %}
    {{ adapter.dispatch('strategy__materialized_view__full_refresh', 'dbt')(relation, sql, backup_relation, intermediate_relation) }}
{% endmacro %}


{% macro default__materialized_view__full_refresh(relation, sql, backup_relation=None, intermediate_relation=None) %}
    {% if backup_relation %}
        {{ db_api__materialized_view__create(intermediate_relation, sql) }}
        {{ adapter.rename_relation(target_relation, backup_relation) }}
        {{ adapter.rename_relation(intermediate_relation, target_relation) }}
        {{ drop_relation_if_exists(backup_relation) }}
    {% else %}
        {{ drop_relation_if_exists(target_relation) }}
        {{ db_api__materialized_view__create(target_relation, sql) }}
    {% endif %}

{% endmacro %}


{% macro materialized_view__refresh(relation) %}
    {{ adapter.dispatch('materialized_view__refresh', 'dbt')(relation) }}
{% endmacro %}

{% macro default__materialized_view__refresh(relation) -%}

    {{ exceptions.raise_not_implemented(
    'materialized_view__refresh not implemented for adapter '+adapter.type()) }}

{% endmacro %}
