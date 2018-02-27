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
    {{ package_context[search_name](*varargs, **kwargs) }}
  {%- else -%}
    {{ package_context[default_name](*varargs, **kwargs) }}
  {%- endif -%}
{%- endmacro %}

{% macro create_schema(schema_name) %}
  create schema if not exists "{{ schema_name }}";
{% endmacro %}


{% macro create_table_as(temporary, identifier, sql) -%}
  {{ adapter_macro('create_table_as', temporary, identifier, sql) }}
{%- endmacro %}

{% macro default__create_table_as(temporary, identifier, sql) -%}
  create {% if temporary: -%}temporary{%- endif %} table "{{ schema }}"."{{ identifier }}" as (
    {{ sql }}
  );
{% endmacro %}

{% macro create_view_as(identifier, sql) -%}
  {{ adapter_macro('create_view_as', identifier, sql) }}
{%- endmacro %}

{% macro default__create_view_as(identifier, sql) -%}
  create view "{{ schema }}"."{{ identifier }}" as (
    {{ sql }}
  );
{% endmacro %}


{% macro create_archive_table(schema, identifier, columns) -%}
  {{ adapter_macro('create_archive_table', schema, identifier, columns) }}
{%- endmacro %}

{% macro default__create_archive_table(schema, identifier, columns) -%}
  create table if not exists "{{ schema }}"."{{ identifier }}" (
    {{ column_list_for_create_table(columns) }}
  );
{% endmacro %}
