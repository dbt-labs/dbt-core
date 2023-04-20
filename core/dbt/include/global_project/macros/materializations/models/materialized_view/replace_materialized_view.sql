{% macro get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{- log('Applying REPLACE to: ' ~ relation) -}}
    {{- adapter.dispatch('get_replace_materialized_view_as_sql', 'dbt')(relation, sql, existing_relation, backup_relation, intermediate_relation) -}}
{% endmacro %}


{% macro default__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{- get_create_view_as_sql(intermediate_relation, sql) -}}

    {% if existing_relation is not none %}
        alter view {{ existing_relation }} rename to {{ backup_relation.include(database=False, schema=False) }};
    {% endif %}

    alter view {{ intermediate_relation }} rename to {{ relation.include(database=False, schema=False) }};

{% endmacro %}
