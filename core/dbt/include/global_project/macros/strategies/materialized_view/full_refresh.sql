{% macro strategy__materialized_view__full_refresh(relation, sql, backup_relation=None, intermediate_relation=None) %}
    {{ adapter.dispatch('strategy__materialized_view__full_refresh', 'dbt')(relation, sql, backup_relation, intermediate_relation) }}
{% endmacro %}


{% macro default__strategy__materialized_view__full_refresh(relation, sql, backup_relation=None, intermediate_relation=None) %}
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
