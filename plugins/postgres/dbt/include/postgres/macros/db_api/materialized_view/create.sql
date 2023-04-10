{% macro postgres__db_api__materialized_view__create(materialized_view_name, sql) %}
    {% set proxy_view = postgres__create_view_as(materialized_view_name, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}
