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


{% macro postgres__get_create_materialized_view_as_sql(relation, sql) %}
    {{- return(get_create_view_as_sql(relation, sql)) -}}
{% endmacro %}


{% macro postgres__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{- get_create_view_as_sql(intermediate_relation, sql) -}}

    {% if existing_relation is not none %}
        alter view {{ existing_relation }} rename to {{ backup_relation.include(database=False, schema=False) }};
    {% endif %}

    alter view {{ intermediate_relation }} rename to {{ relation.include(database=False, schema=False) }};

{% endmacro %}


{% macro default__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {%- do return([]) -%}
{% endmacro %}


{% macro postgres__refresh_materialized_view(relation) %}
    {{- '' -}}
{% endmacro %}
