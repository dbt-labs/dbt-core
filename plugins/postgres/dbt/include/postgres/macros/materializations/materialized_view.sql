{% macro postgres__create_materialized_view_as(relation, sql) %}
    {% set proxy_view = postgres_create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}


{% macro postgres__refresh_materialized_view(relation) %}
    {{ return({'relations': [relation]}) }}
{% endmacro %}
