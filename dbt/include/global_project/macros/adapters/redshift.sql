{% macro dist(dist) %}
  {%- set dist = dist.strip().lower() -%}
  {%- if dist is none -%}

  {%- elif dist in ['all', 'even'] -%}
    diststyle {{ dist }}
  {%- else -%}
    diststyle key distkey ("{{ dist }}")
  {%- endif -%}
{%- endmacro -%}


{% macro sort(sort_type, sort) %}
  {{ sort_type }} sortkey(
  {%- if sort is string -%}
    {%- set sort = [sort] -%}
  {%- endif -%}
  {%- for item in sort -%}
    "{{ item }}"
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{%- endmacro -%}


{% macro redshift__create_table_as(temporary, identifier, sql, config) -%}

  {%- set dist = config.get('dist') -%}
  {%- set sort_type = config.get(
          'sort_type',
          validator=validation.any['compound', 'interleaved']) -%}
  {%- set sort = config.get(
          'sort',
          validator=validation.any[list, basestring]) -%}

  create {% if temporary -%}temporary{%- endif %} table "{{ schema }}"."{{ identifier }}"
  {{ dist(dist) }}
  {{ sort(sort_type, sort) }}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro redshift__create_archive_table(schema, identifier, columns) -%}
  create table if not exists "{{ schema }}"."{{ identifier }}" (
    {{ column_list_for_create_table(columns) }}
  )
  {{ dist('dbt_updated_at') }}
  {{ sort('compound', ['scd_id']) }};
{%- endmacro %}
