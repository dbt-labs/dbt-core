{% macro adapter_macro(name) -%}
{% set original_name = name %}
  {% if '.' in name %}
    {% set package_name, name = name.split(".", 1) %}
  {% else %}
    {% set package_name = none %}
  {% endif %}

  {% if package_name is none %}
    {% set package_context = context %}
  {% elif package_name in context %}
    {% set package_context = context[package_name] %}
  {% else %}
    {% set error_msg %}
        In adapter_macro: could not find package '{{package_name}}', called with '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}

  {%- set separator = '__' -%}
  {%- set search_name = adapter.type() + separator + name -%}
  {%- set default_name = 'default' + separator + name -%}

  {%- if package_context.get(search_name) is not none -%}
    {{ return(package_context[search_name](*varargs, **kwargs)) }}
  {%- else -%}
    {{ return(package_context[default_name](*varargs, **kwargs)) }}
  {%- endif -%}
{%- endmacro %}

{% macro create_schema(database_name, schema_name) %}
  {{ adapter.create_schema(database_name, schema_name) }}
{% endmacro %}

{% macro create_table_as(temporary, relation, sql) -%}
  {{ adapter_macro('create_table_as', temporary, relation, sql) }}
{%- endmacro %}

{% macro default__create_table_as(temporary, relation, sql) -%}
  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  as (
    {{ sql }}
  );
{% endmacro %}


{% macro create_view_as(relation, sql) -%}
  {{ adapter_macro('create_view_as', relation, sql) }}
{%- endmacro %}

{% macro default__create_view_as(relation, sql) -%}
  create view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}


{% macro create_archive_table(relation, columns) -%}
  {{ adapter_macro('create_archive_table', relation, columns) }}
{%- endmacro %}

{% macro default__create_archive_table(relation, columns) -%}
  create table if not exists {{ relation }} (
    {{ column_list_for_create_table(columns) }}
  );
{% endmacro %}


{% macro get_catalog() -%}
  {{ return(adapter_macro('get_catalog')) }}
{%- endmacro %}

{% macro default__get_catalog() -%}

  {% set typename = adapter.type() %}
  {% set msg -%}
    get_catalog not implemented for {{ typename }}
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro get_relations() -%}
  {{ return(adapter_macro('get_relations')) }}
{% endmacro %}


{% macro default__get_relations() -%}
  {% set typename = adapter.type() %}
  {% set msg -%}
    get_relations not implemented for {{ typename }}
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}
