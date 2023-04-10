{% macro postgres__strategy__materialized_view__refresh_data(relation) %}
    {{ postgres__db_api__materialized_view__refresh(str(relation)) }}
{% endmacro %}
