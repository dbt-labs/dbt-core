{% macro postgres__get_create_materialized_view_as_sql(relation, sql) %}
    {{ return(get_create_view_as_sql(relation, sql)) }}
{% endmacro %}


{% macro postgres__get_refresh_data_in_materialized_view_sql(relation) %}
    select 1;
{% endmacro %}


{% macro postgres__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ get_create_view_as_sql(intermediate_relation, sql) }}

    {% if existing_relation is not none %}
        alter view {{ existing_relation }} rename to {{ backup_relation.include(database=False, schema=False) }};
    {% endif %}

    alter view {{ intermediate_relation }} rename to {{ relation.include(database=False, schema=False) }};

{% endmacro %}


{% macro postgres__get_alter_materialized_view_sql(relation, updates, sql, existing_relation, backup_relation, intermediate_relation) %}
    {% if 'index' in updates.keys() %}
        select 1;
    {% else %}
        {{ postgres__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) }}
    {% endif %}
{% endmacro %}
