{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }}
  as (
    {{ sql }}
  );

  {% set relation = relation.incorporate(type='table') %}
  {{ set_relation_comment(relation) }}
  {{ set_column_comments(relation) }}
{%- endmacro %}


{% macro postgres__create_view_as(relation, sql) %}
  {{ default__create_view_as(relation, sql) }}
  {%- set relation = relation.incorporate(type='view') -%}
  {{ set_relation_comment(relation) }}
  {{ set_column_comments(relation) }}
{% endmacro %}

{% macro postgres__create_schema(database_name, schema_name) -%}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ schema_name }}
  {%- endcall -%}
{% endmacro %}

{% macro postgres__drop_schema(database_name, schema_name) -%}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ schema_name }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro postgres__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from {{ relation.information_schema('columns') }}
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro postgres__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ information_schema.database }}' as database,
      tablename as name,
      schemaname as schema,
      'table' as type
    from pg_tables
    where schemaname ilike '{{ schema }}'
    union all
    select
      '{{ information_schema.database }}' as database,
      viewname as name,
      schemaname as schema,
      'view' as type
    from pg_views
    where schemaname ilike '{{ schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro postgres__information_schema_name(database) -%}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  information_schema
{%- endmacro %}

{% macro postgres__list_schemas(database) %}
  {% if database -%}
    {{ adapter.verify_database(database) }}
  {%- endif -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct nspname from pg_namespace
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro postgres__check_schema_exists(information_schema, schema) -%}
  {% if information_schema.database -%}
    {{ adapter.verify_database(information_schema.database) }}
  {%- endif -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    select count(*) from pg_namespace where nspname = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro postgres__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro postgres__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamp without time zone" -%}
    {{ return(result) }}
{%- endmacro %}


{% macro postgres__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp without time zone
{%- endmacro %}

{% macro postgres__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": none,
                                    "database": none
                                  })) -%}
{% endmacro %}


{#
  By using dollar-quoting like this, users can embed anything they want into their comments
  (including nested dollar-quoting), as long as they don't use this exact dollar-quoting
  label. It would be nice to just pick a new one but eventually you do have to give up.
#}
{% macro postgres_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_exception('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_exception('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}


{% macro postgres__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = postgres_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}


{% macro postgres__alter_column_comment(relation, column_dict) %}
  {% for column_name in column_dict %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = postgres_escape_comment(comment) %}
    comment on column {{ relation }}.{{ column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}
