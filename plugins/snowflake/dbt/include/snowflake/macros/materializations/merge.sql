{% macro snowflake__get_merge_sql(target, source, unique_key, dest_columns) %}
    {{ common_get_merge_sql(target, source, unique_key, dest_columns) }}
{% endmacro %}
