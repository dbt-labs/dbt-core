{# /*
This only exists for backwards compatibility for 1.6.0. In later versions, the general `get_replace_sql`
macro is called as replace is inherently not limited to a single relation (it takes in two relations).
*/ #}


{% macro postgres__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{- get_create_materialized_view_as_sql(intermediate_relation, sql) -}}

    {% if existing_relation is not none %}
        alter materialized view {{ existing_relation }} rename to {{ backup_relation.include(database=False, schema=False) }};
    {% endif %}

    alter materialized view {{ intermediate_relation }} rename to {{ relation.include(database=False, schema=False) }};

{% endmacro %}
