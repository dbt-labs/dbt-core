{% macro dist(dist) %}
  {%- if dist is not none -%}
      {%- set dist = dist.strip().lower() -%}

      {%- if dist in ['all', 'even'] -%}
        diststyle {{ dist }}
      {%- else -%}
        diststyle key distkey ("{{ dist }}")
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
        "{{ item }}"
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
      )
  {%- endif %}
{%- endmacro -%}


{% macro redshift__create_table_as(temporary, identifier, sql) -%}

  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get(
          'sort_type',
          validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get(
          'sort',
          validator=validation.any[list, basestring]) -%}

  {% if temporary %}
    {% set relation = adapter.quote(identifier) %}
  {% else %}
    {% set relation = adapter.quote(schema) ~ '.' ~ adapter.quote(identifier) %}
  {% endif %}

  create {% if temporary -%}temporary{%- endif %} table {{ relation }}
  {{ dist(_dist) }}
  {{ sort(_sort_type, _sort) }}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro redshift__create_view_as(identifier, sql) -%}

  {% set bind_qualifier = '' if config.get('bind', default=True) else 'with no schema binding' %}

  create view "{{ schema }}"."{{ identifier }}" as (
    {{ sql }}
  ) {{ bind_qualifier }};
{% endmacro %}


{% macro redshift__create_archive_table(schema, identifier, columns) -%}
  create table if not exists "{{ schema }}"."{{ identifier }}" (
    {{ column_list_for_create_table(columns) }}
  )
  {{ dist('dbt_updated_at') }}
  {{ sort('compound', ['scd_id']) }};
{%- endmacro %}
