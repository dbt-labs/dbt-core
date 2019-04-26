
{% macro redshift__archive_merge_sql(target, source, insert_cols) -%}
    {{ postgres__archive_merge_sql(target, source, insert_cols) }}
{% endmacro %}
