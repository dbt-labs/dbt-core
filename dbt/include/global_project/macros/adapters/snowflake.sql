{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {% if temporary %}
    use schema {{ adapter.quote_as_configured(schema, 'schema') }};
  {% endif %}

  {{ default__create_table_as(temporary, relation, sql) }}
{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__swap_table(old_relation, new_relation) -%}
  alter table {{ old_relation }} swap with {{ new_relation }}
  ;
{% endmacro %}
