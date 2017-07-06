{% macro adapter_macro(name) -%}
  {%- set separator = '__' -%}
  {%- set search_name = adapter.type() + separator + name -%}
  {%- set default_name = 'default' + separator + name -%}
  {%- if context.get(search_name) is not none -%}
    {{ context[search_name](*varargs, **kwargs) }}
  {%- else -%}
    {{ context[default_name](*varargs, **kwargs) }}
  {%- endif -%}
{%- endmacro %}


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
