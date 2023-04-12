{#
    These macros use the default:
        postgres__get_create_materialized_view_as_sql
        postgres__get_refresh_data_in_materialized_view_sql
        postgres__get_replace_materialized_view_as_sql
#}


{% macro postgres__get_alter_materialized_view_sql(relation, updates, sql, existing_relation, backup_relation, intermediate_relation) %}
    {% if 'indexes' in updates.keys() %}
        {% for index in updates.get('indexes') %}
            {{ postgres__get_drop_index_sql(relation, index.get('definition')) }}
            {% if not index.get('action') == 'drop' %}
                {{ get_create_index_sql(relation, index.get('definition')) }}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) }}
    {% endif %}
{% endmacro %}
