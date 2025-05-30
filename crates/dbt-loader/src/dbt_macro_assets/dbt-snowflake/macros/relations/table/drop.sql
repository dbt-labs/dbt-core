{% macro snowflake__get_drop_table_sql(relation) %}
    drop table if exists {{ relation }} cascade
{% endmacro %}
