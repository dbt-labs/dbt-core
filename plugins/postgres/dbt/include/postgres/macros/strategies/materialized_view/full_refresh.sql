{% macro redshift__strategy__materialized_view__full_refresh(relation, sql) %}
    {{ drop_relation_if_exists(relation) }}
    {{ redshift__db_api__materialized_view__create(str(relation), sql) }}
{% endmacro %}
