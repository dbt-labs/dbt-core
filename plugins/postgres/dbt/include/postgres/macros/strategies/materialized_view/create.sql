{% macro postgres__strategy__materialized_view__create(relation, sql) %}
    {{ postgres__db_api__materialized_view__create(str(relation), sql) }}
{% endmacro %}
