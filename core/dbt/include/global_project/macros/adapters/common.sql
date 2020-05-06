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

{% macro get_columns_in_query(select_sql) -%}
  {{ return(adapter_macro('get_columns_in_query', select_sql)) }}
{% endmacro %}

{% macro default__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select * from (
            {{ select_sql }}
        ) as __dbt_sbq
        where false
        limit 0
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro create_schema(relation) -%}
  {{ adapter_macro('create_schema', relation) }}
{% endmacro %}

{% macro default__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro drop_schema(relation) -%}
  {{ adapter_macro('drop_schema', relation) }}
{% endmacro %}

{% macro default__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {% endcall %}
{% endmacro %}

{% macro create_table_as(temporary, relation, sql) -%}
  {{ adapter_macro('create_table_as', temporary, relation, sql) }}
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

{% macro create_view_as(relation, sql) -%}
  {{ adapter_macro('create_view_as', relation, sql) }}
{%- endmacro %}

{% macro default__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}


{% macro get_catalog(information_schema, schemas) -%}
  {{ return(adapter_macro('get_catalog', information_schema, schemas)) }}
{%- endmacro %}

{% macro default__get_catalog(information_schema, schemas) -%}

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
  {{ exceptions.raise_not_implemented(
    'get_columns_in_relation macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro alter_column_type(relation, column_name, new_column_type) -%}
  {{ return(adapter_macro('alter_column_type', relation, column_name, new_column_type)) }}
{% endmacro %}



{% macro alter_column_comment(relation, column_dict) -%}
  {{ return(adapter_macro('alter_column_comment', relation, column_dict)) }}
{% endmacro %}

{% macro default__alter_column_comment(relation, column_dict) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro alter_relation_comment(relation, relation_comment) -%}
  {{ return(adapter_macro('alter_relation_comment', relation, relation_comment)) }}
{% endmacro %}

{% macro default__alter_relation_comment(relation, relation_comment) -%}
  {{ exceptions.raise_not_implemented(
    'alter_relation_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro persist_docs(relation, model, for_relation=true, for_columns=true) -%}
  {{ return(adapter_macro('persist_docs', relation, model, for_relation, for_columns)) }}
{% endmacro %}

{% macro default__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {% if for_columns and config.persist_column_docs() %}
    {% do run_query(alter_column_comment(relation, model.columns)) %}
  {% endif %}
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
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }} cascade;
    alter table {{ relation }} rename column {{ adapter.quote(tmp_column) }} to {{ adapter.quote(column_name) }}
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
    {{ database }}.INFORMATION_SCHEMA
  {%- else -%}
    INFORMATION_SCHEMA
  {%- endif -%}
{%- endmacro %}


{% macro list_schemas(database) -%}
  {{ return(adapter_macro('list_schemas', database)) }}
{% endmacro %}

{% macro default__list_schemas(database) -%}
  {% set sql %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.SCHEMATA
    where catalog_name ilike '{{ database }}'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro check_schema_exists(information_schema, schema) -%}
  {{ return(adapter_macro('check_schema_exists', information_schema, schema)) }}
{% endmacro %}

{% macro default__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from {{ information_schema.replace(information_schema_view='SCHEMATA') }}
        where catalog_name='{{ information_schema.database }}'
          and schema_name='{{ schema }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro list_relations_without_caching(schema_relation) %}
  {{ return(adapter_macro('list_relations_without_caching', schema_relation)) }}
{% endmacro %}


{% macro default__list_relations_without_caching(schema_relation) %}
  {{ exceptions.raise_not_implemented(
    'list_relations_without_caching macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}


{% macro current_timestamp() -%}
  {{ adapter_macro('current_timestamp') }}
{%- endmacro %}


{% macro default__current_timestamp() -%}
  {{ exceptions.raise_not_implemented(
    'current_timestamp macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{% macro collect_freshness(source, loaded_at_field, filter) %}
  {{ return(adapter_macro('collect_freshness', source, loaded_at_field, filter))}}
{% endmacro %}


{% macro default__collect_freshness(source, loaded_at_field, filter) %}
  {% call statement('collect_freshness', fetch_result=True, auto_begin=False) -%}
    select
      max({{ loaded_at_field }}) as max_loaded_at,
      {{ current_timestamp() }} as snapshotted_at
    from {{ source }}
    {% if filter %}
    where {{ filter }}
    {% endif %}
  {% endcall %}
  {{ return(load_result('collect_freshness').table) }}
{% endmacro %}

{% macro make_temp_relation(base_relation, suffix='__dbt_tmp') %}
  {{ return(adapter_macro('make_temp_relation', base_relation, suffix))}}
{% endmacro %}

{% macro default__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier}) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro set_sql_header(config) -%}
  {{ config.set('sql_header', caller()) }}
{%- endmacro %}
