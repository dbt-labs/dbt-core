{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {% if temporary %}
    use schema {{ adapter.quote_as_configured(relation.schema, 'schema') }};
  {% endif %}

  {{ default__create_table_as(temporary, relation, sql) }}
{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}
