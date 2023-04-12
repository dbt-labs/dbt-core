{% macro get_alter_materialized_view_as_sql(relation, updates, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ adapter.dispatch('get_alter_materialized_view_as_sql', 'dbt')(relation, updates, sql, existing_relation, backup_relation, intermediate_relation) }}
{% endmacro %}


{% macro default__get_alter_materialized_view_as_sql(relation, updates, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ return(get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation)) }}
{% endmacro %}
