{% macro motherduck__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {% if language != 'sql' %}
    {{ exceptions.raise_compiler_error('Python models are not supported by dbt-motherduck') }}
  {% endif %}
  {{ return(duckdb__create_table_as(temporary, relation, compiled_code, language)) }}
{%- endmacro %}

{% macro motherduck__list_schemas(database) -%}
  {% set sql %}
    select schema_name
    from duckdb_schemas()
    {% if database is not none %}
    where lower(database_name) = '{{ database | lower | replace("'", "''") }}'
    {% endif %}
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro motherduck__check_schema_exists(information_schema, schema) -%}
  {% set sql %}
    select count(*)
    from duckdb_schemas()
    where lower(schema_name) = '{{ schema | lower | replace("'", "''") }}'
      and lower(database_name) = '{{ information_schema.database | lower | replace("'", "''") }}'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro motherduck__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database | replace("'", "''") }}' as database,
      table_name as name,
      schema_name as schema,
      'table' as type
    from duckdb_tables()
    where lower(schema_name) = '{{ schema_relation.schema | lower | replace("'", "''") }}'
      and lower(database_name) = '{{ schema_relation.database | lower | replace("'", "''") }}'
    union all
    select
      '{{ schema_relation.database | replace("'", "''") }}' as database,
      view_name as name,
      schema_name as schema,
      'view' as type
    from duckdb_views()
    where lower(schema_name) = '{{ schema_relation.schema | lower | replace("'", "''") }}'
      and lower(database_name) = '{{ schema_relation.database | lower | replace("'", "''") }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro motherduck__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True, auto_begin=False) %}
    select
      column_name,
      column_type as data_type,
      cast(null as bigint) as character_maximum_length,
      cast(null as bigint) as numeric_precision,
      cast(null as bigint) as numeric_scale
    from (describe {% if relation.external %}select * from {% endif %}{{ relation.render() }})
  {% endcall %}
  {{ return(sql_convert_columns_in_relation(load_result('get_columns_in_relation').table)) }}
{% endmacro %}
