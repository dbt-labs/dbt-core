{% macro get_replace_materialized_view_as_sql(relation, sql, backup_relation, intermediate_relation) %}
    {{ adapter.dispatch('get_replace_materialized_view_as_sql', 'dbt')(relation, sql, backup_relation, intermediate_relation) }}
{% endmacro %}


{% macro default__get_replace_materialized_view_as_sql(relation, sql, backup_relation, intermediate_relation) %}
    {{ return(get_create_or_replace_view_as_sql(relation, sql)) }}
{% endmacro %}
