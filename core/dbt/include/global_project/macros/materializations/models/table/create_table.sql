{% macro create_table_as(temporary, relation, sql) -%}
  {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, sql) }}
{%- endmacro %}

{% macro default__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  as (
    {{ sql }}
  );

{% endmacro %}
