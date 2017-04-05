{% macro dbt__create_table(schema, model, dist, sort, sql, flags, adapter) -%}

  {%- set identifier = model['name'] -%}
  {%- set already_exists = adapter.already_exists(schema, identifier) -%}
  {%- set non_destructive_mode = flags.NON_DESTRUCTIVE == True -%}

  {% if non_destructive_mode and already_exists -%}
    create temporary table {{ identifier }}__dbt_tmp {{ dist }} {{ sort }} as (
      {{ sql }}
    );

    {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
    {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

    insert into {{ schema }}.{{ identifier }} ({{ dest_cols_csv }})
    (
      select {{ dest_cols_csv }}
      from "{{ identifier }}__dbt_tmp"
    );
  {%- elif non_destructive_mode -%}
    create table "{{ schema }}"."{{ identifier }}"
      {{ dist }} {{ sort }} as (
        {{ sql }}
    );
  {%- else -%}
    create table "{{ schema }}"."{{ identifier }}__dbt_tmp"
      {{ dist }} {{ sort }} as (
        {{ sql }}
    );
  {%- endif %}

{%- endmacro %}
