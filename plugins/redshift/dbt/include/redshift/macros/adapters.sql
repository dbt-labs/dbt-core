{% macro dist(dist) %}
  {%- if dist is not none -%}
      {%- set dist = dist.strip().lower() -%}

      {%- if dist in ['all', 'even'] -%}
        diststyle {{ dist }}
      {%- else -%}
        diststyle key distkey ({{ dist }})
      {%- endif -%}

  {%- endif -%}
{%- endmacro -%}


{% macro sort(sort_type, sort) %}
  {%- if sort is not none %}
      {{ sort_type | default('compound', boolean=true) }} sortkey(
      {%- if sort is string -%}
        {%- set sort = [sort] -%}
      {%- endif -%}
      {%- for item in sort -%}
        {{ item }}
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
      )
  {%- endif %}
{%- endmacro -%}


{% macro redshift__create_table_as(temporary, relation, sql) -%}

  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get(
          'sort_type',
          validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get(
          'sort',
          validator=validation.any[list, basestring]) -%}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {{ dist(_dist) }}
    {{ sort(_sort_type, _sort) }}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro redshift__create_view_as(relation, sql) -%}

  {% set bind_qualifier = '' if config.get('bind', default=True) else 'with no schema binding' %}

  create view {{ relation }} as (
    {{ sql }}
  ) {{ bind_qualifier }};
{% endmacro %}


{% macro redshift__create_archive_table(relation, columns) -%}
  create table if not exists {{ relation }} (
    {{ column_list_for_create_table(columns) }}
  )
  {{ dist('dbt_updated_at') }}
  {{ sort('compound', ['scd_id']) }};
{%- endmacro %}


{% macro redshift__create_schema(database_name, schema_name) -%}
  {{ postgres__create_schema(database_name, schema_name) }}
{% endmacro %}


{% macro redshift__drop_schema(database_name, schema_name) -%}
  {{ postgres__drop_schema(database_name, schema_name) }}
{% endmacro %}


{% macro redshift__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      with bound_views as (
        select
          ordinal_position,
          table_schema,
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

        from {{ relation.information_schema('columns') }}
        where table_name = '{{ relation.identifier }}'
    ),

    unbound_views as (
      select
        ordinal_position,
        view_schema,
        col_name,
        case
          when col_type ilike 'character varying%' then
            'character varying'
          when col_type ilike 'numeric%' then 'numeric'
          else col_type
        end as col_type,
        case
          when col_type like 'character%'
          then nullif(REGEXP_SUBSTR(col_type, '[0-9]+'), '')::int
          else null
        end as character_maximum_length,
        case
          when col_type like 'numeric%'
          then nullif(
            SPLIT_PART(REGEXP_SUBSTR(col_type, '[0-9,]+'), ',', 1),
            '')::int
          else null
        end as numeric_precision,
        case
          when col_type like 'numeric%'
          then nullif(
            SPLIT_PART(REGEXP_SUBSTR(col_type, '[0-9,]+'), ',', 2),
            '')::int
          else null
        end as numeric_scale

      from pg_get_late_binding_view_cols()
      cols(view_schema name, view_name name, col_name name,
           col_type varchar, ordinal_position int)
      where view_name = '{{ relation.identifier }}'
    ),

    unioned as (
      select * from bound_views
      union all
      select * from unbound_views
    )

    select
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale

    from unioned
    {% if relation.schema %}
    where table_schema = '{{ relation.schema }}'
    {% endif %}
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro redshift__list_relations_without_caching(information_schema, schema) %}
  {{ return(postgres__list_relations_without_caching(information_schema, schema)) }}
{% endmacro %}


{% macro redshift__information_schema_name(database) -%}
  {{ return(postgres__information_schema_name(database)) }}
{%- endmacro %}


{% macro redshift__list_schemas(database) -%}
  {{ return(postgres__list_schemas(database)) }}
{%- endmacro %}


{% macro redshift__check_schema_exists(information_schema, schema) -%}
  {{ return(postgres__check_schema_exists(information_schema, schema)) }}
{%- endmacro %}
list_schemas

%}

{% macro redshift__current_timestamp() -%}
  getdate()
{%- endmacro %}
