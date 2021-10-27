{% macro get_columns_in_query(select_sql) -%}
  {{ return(adapter.dispatch('get_columns_in_query', 'dbt')(select_sql)) }}
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
  {{ adapter.dispatch('create_schema', 'dbt')(relation) }}
{% endmacro %}

{% macro default__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro drop_schema(relation) -%}
  {{ adapter.dispatch('drop_schema', 'dbt')(relation) }}
{% endmacro %}

{% macro default__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {% endcall %}
{% endmacro %}

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

{% macro create_view_as(relation, sql) -%}
  {{ adapter.dispatch('create_view_as', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro default__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}


{% macro get_catalog(information_schema, schemas) -%}
  {{ return(adapter.dispatch('get_catalog', 'dbt')(information_schema, schemas)) }}
{%- endmacro %}

{% macro default__get_catalog(information_schema, schemas) -%}

  {% set typename = adapter.type() %}
  {% set msg -%}
    get_catalog not implemented for {{ typename }}
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}


{% macro get_columns_in_relation(relation) -%}
  {{ return(adapter.dispatch('get_columns_in_relation', 'dbt')(relation)) }}
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
  {{ return(adapter.dispatch('alter_column_type', 'dbt')(relation, column_name, new_column_type)) }}
{% endmacro %}



{% macro alter_column_comment(relation, column_dict) -%}
  {{ return(adapter.dispatch('alter_column_comment', 'dbt')(relation, column_dict)) }}
{% endmacro %}

{% macro default__alter_column_comment(relation, column_dict) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro alter_relation_comment(relation, relation_comment) -%}
  {{ return(adapter.dispatch('alter_relation_comment', 'dbt')(relation, relation_comment)) }}
{% endmacro %}

{% macro default__alter_relation_comment(relation, relation_comment) -%}
  {{ exceptions.raise_not_implemented(
    'alter_relation_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro persist_docs(relation, model, for_relation=true, for_columns=true) -%}
  {{ return(adapter.dispatch('persist_docs', 'dbt')(relation, model, for_relation, for_columns)) }}
{% endmacro %}

{% macro default__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {% if for_columns and config.persist_column_docs() and model.columns %}
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
  {{ return(adapter.dispatch('drop_relation', 'dbt')(relation)) }}
{% endmacro %}


{% macro default__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro truncate_relation(relation) -%}
  {{ return(adapter.dispatch('truncate_relation', 'dbt')(relation)) }}
{% endmacro %}


{% macro default__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro rename_relation(from_relation, to_relation) -%}
  {{ return(adapter.dispatch('rename_relation', 'dbt')(from_relation, to_relation)) }}
{% endmacro %}

{% macro default__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}


{% macro information_schema_name(database) %}
  {{ return(adapter.dispatch('information_schema_name', 'dbt')(database)) }}
{% endmacro %}

{% macro default__information_schema_name(database) -%}
  {%- if database -%}
    {{ database }}.INFORMATION_SCHEMA
  {%- else -%}
    INFORMATION_SCHEMA
  {%- endif -%}
{%- endmacro %}


{% macro list_schemas(database) -%}
  {{ return(adapter.dispatch('list_schemas', 'dbt')(database)) }}
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
  {{ return(adapter.dispatch('check_schema_exists', 'dbt')(information_schema, schema)) }}
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
  {{ return(adapter.dispatch('list_relations_without_caching', 'dbt')(schema_relation)) }}
{% endmacro %}


{% macro default__list_relations_without_caching(schema_relation) %}
  {{ exceptions.raise_not_implemented(
    'list_relations_without_caching macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}


{% macro current_timestamp() -%}
  {{ adapter.dispatch('current_timestamp', 'dbt')() }}
{%- endmacro %}


{% macro default__current_timestamp() -%}
  {{ exceptions.raise_not_implemented(
    'current_timestamp macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


{% macro collect_freshness(source, loaded_at_field, filter) %}
  {{ return(adapter.dispatch('collect_freshness', 'dbt')(source, loaded_at_field, filter))}}
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
  {{ return(adapter.dispatch('make_temp_relation', 'dbt')(base_relation, suffix))}}
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


{% macro alter_relation_add_remove_columns(relation, add_columns = none, remove_columns = none) -%}
  {{ return(adapter.dispatch('alter_relation_add_remove_columns', 'dbt')(relation, add_columns, remove_columns)) }}
{% endmacro %}

{% macro default__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  
  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}
  
  {% set sql -%}
     
     alter {{ relation.type }} {{ relation }}
       
            {% for column in add_columns %}
               add column {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
            {% endfor %}{{ ',' if add_columns and remove_columns }}
            
            {% for column in remove_columns %}
                drop column {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
  
  {%- endset -%}

  {% do run_query(sql) %}

{% endmacro %}
