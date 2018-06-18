{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {% if temporary %}
    use schema {{ schema }};
  {% endif %}

  {{ default__create_table_as(temporary, relation, sql) }}
{% endmacro %}
