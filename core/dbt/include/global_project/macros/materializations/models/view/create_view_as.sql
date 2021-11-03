{% macro get_create_view_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_view_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro default__get_create_view_as_sql(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}


/* {# old name for backwards compatibility #} */
{% macro create_view_as(relation, sql) -%}
  {{ adapter.dispatch('create_view_as', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro default__create_view_as(relation, sql) -%}
  {{ return(get_create_view_as_sql(relation, sql)) }}
{%- endmacro %}
