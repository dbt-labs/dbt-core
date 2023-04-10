{% macro postgres__db_api__materialized_view__refresh(materialized_view_name) %}
    {{ return({'relations': [materialized_view_name]}) }}
{% endmacro %}
