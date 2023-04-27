{% macro get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    {{- log('Applying ALTER to: ' ~ relation) -}}
    {{- adapter.dispatch('get_alter_materialized_view_as_sql', 'dbt')(
        relation,
        configuration_changes,
        sql,
        existing_relation,
        backup_relation,
        intermediate_relation
    ) -}}
{% endmacro %}


{% macro default__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    {{- return(get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation)) -}}
{% endmacro %}
