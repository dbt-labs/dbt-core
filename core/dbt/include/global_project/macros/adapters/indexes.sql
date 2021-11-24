{% macro get_create_index_sql(relation, index_dict) -%}
  {{ return(adapter.dispatch('get_create_index_sql', 'dbt')(relation, index_dict)) }}
{% endmacro %}

{% macro default__get_create_index_sql(relation, index_dict) -%}
  {% do return(None) %}
{% endmacro %}


{% macro create_indexes(relation) -%}
  {{ adapter.dispatch('create_indexes', 'dbt')(relation) }}
{%- endmacro %}

{% macro default__create_indexes(relation) -%}
  {%- set _indexes = config.get('indexes', default=[]) -%}

  {% for _index_dict in _indexes %}
    {% set create_index_sql = get_create_index_sql(relation, _index_dict) %}
    {% if create_index_sql %}
      {% do run_query(create_index_sql) %}
    {% endif %}
  {% endfor %}
{% endmacro %}
