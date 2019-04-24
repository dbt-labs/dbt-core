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

{% macro create_schema(database_name, schema_name) -%}
  {{ adapter_macro('create_schema', database_name, schema_name) }}
{% endmacro %}

{% macro default__create_schema(database_name, schema_name) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{database_name}}.{{schema_name}}
  {% endcall %}
{% endmacro %}

{% macro drop_schema(database_name, schema_name) -%}
  {{ adapter_macro('drop_schema', database_name, schema_name) }}
{% endmacro %}

{% macro default__drop_schema(database_name, schema_name) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{database_name}}.{{schema_name}} cascade
  {% endcall %}
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


{% macro get_catalog(information_schemas) -%}
  {{ return(adapter_macro('get_catalog', information_schemas)) }}
{%- endmacro %}

{% macro default__get_catalog(information_schemas) -%}

  {% set typename = adapter.type() %}
  {% set msg -%}
    get_catalog not implemented for {{ typename }}
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}


{% macro get_columns_in_relation(relation) -%}
  {{ return(adapter_macro('get_columns_in_relation', relation)) }}
{% endmacro %}

{% macro sql_convert_columns_in_relation(table) -%}
  {% set columns = [] %}
  {% for row in table %}
    {% do columns.append(api.Column(*row)) %}
  {% endfor %}
  {{ return(columns) }}
{% endmacro %}

{% macro default__get_columns_in_relation(relation) -%}
  {{ dbt.exceptions.raise_not_implemented(
    'get_columns_in_relation macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro alter_column_type(relation, column_name, new_column_type) -%}
  {{ return(adapter_macro('alter_column_type', relation, column_name, new_column_type)) }}
{% endmacro %}

{% macro default__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ tmp_column }} {{ new_column_type }};
    update {{ relation }} set {{ tmp_column }} = {{ column_name }};
    alter table {{ relation }} drop column {{ column_name }} cascade;
    alter table {{ relation }} rename column {{ tmp_column }} to {{ column_name }}
  {% endcall %}

{% endmacro %}


{% macro drop_relation(relation) -%}
  {{ return(adapter_macro('drop_relation', relation)) }}
{% endmacro %}


{% macro default__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro truncate_relation(relation) -%}
  {{ return(adapter_macro('truncate_relation', relation)) }}
{% endmacro %}


{% macro default__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro rename_relation(from_relation, to_relation) -%}
  {{ return(adapter_macro('rename_relation', from_relation, to_relation)) }}
{% endmacro %}

{% macro default__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}


{% macro information_schema_name(database) %}
  {{ return(adapter_macro('information_schema_name', database)) }}
{% endmacro %}

{% macro default__information_schema_name(database) -%}
  {%- if database -%}
    {{ adapter.quote_as_configured(database, 'database') }}.information_schema
  {%- else -%}
    information_schema
  {%- endif -%}
{%- endmacro %}


{% macro list_schemas(database) -%}
  {{ return(adapter_macro('list_schemas', database)) }}
{% endmacro %}

{% macro default__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.schemata
    where catalog_name ilike '{{ database }}'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro check_schema_exists(information_schema, schema) -%}
  {{ return(adapter_macro('check_schema_exists', information_schema, schema)) }}
{% endmacro %}

{% macro default__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
        select count(*)
        from {{ information_schema }}.schemata
        where catalog_name='{{ information_schema.database }}'
          and schema_name='{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro list_relations_without_caching(information_schema, schema) %}
  {{ return(adapter_macro('list_relations_without_caching', information_schema, schema)) }}
{% endmacro %}


{% macro default__list_relations_without_caching(information_schema, schema) %}
  {{ dbt.exceptions.raise_not_implemented(
    'list_relations_without_caching macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}


{% macro current_timestamp() -%}
  {{ adapter_macro('current_timestamp') }}
{%- endmacro %}


{% macro default__current_timestamp() -%}
  {{ dbt.exceptions.raise_not_implemented(
    'current_timestamp macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{% macro collect_freshness(source, loaded_at_field) %}
  {{ return(adapter_macro('collect_freshness', source, loaded_at_field))}}
{% endmacro %}


{% macro default__collect_freshness(source, loaded_at_field) %}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    select
      max({{ loaded_at_field }}) as max_loaded_at,
      {{ current_timestamp() }} as snapshotted_at
    from {{ source }}
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}
