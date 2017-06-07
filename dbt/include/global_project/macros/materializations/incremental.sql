{% macro dbt__incremental_delete(schema, model) -%}

  {%- set unique_key = model['config'].get('unique_key') -%}
  {%- set identifier = model['name'] -%}

  delete
  from "{{ schema }}"."{{ identifier }}"
  where ({{ unique_key }}) in (
    select ({{ unique_key }})
    from "{{ identifier }}__dbt_incremental_tmp"
  );

{%- endmacro %}

{% macro dbt__create_incremental(schema, model, dist, sort, sql, flags, adapter) -%}

  {%- set identifier = model['name'] -%}
  {%- set sql_where = model['config'].get('sql_where', 'null') -%}
  {%- set unique_key = model['config'].get('unique_key', 'null') -%}

  {%- set force_create = (flags.FULL_REFRESH and not flags.NON_DESTRUCTIVE) -%}

  {% if force_create or not adapter.already_exists(schema, identifier) -%}

    create table "{{ schema }}"."{{ identifier }}" {{ dist }} {{ sort }} as (
      {{ sql }}
    );

  {%- else -%}

    create temporary table "{{ identifier }}__dbt_incremental_tmp" as (
      with dbt_incr_sbq as (
        {{ sql }}
      )
      select * from dbt_incr_sbq
      where ({{ sql_where }})
        or ({{ sql_where }}) is null
      );

    -- DBT_OPERATION { function: expand_column_types_if_needed, args: { temp_table: "{{ identifier }}__dbt_incremental_tmp", to_schema: "{{ schema }}", to_table: "{{ identifier }}"} }

    {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
    {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

    {% if model.get('config', {}).get('unique_key') is not none -%}

      {{ dbt__incremental_delete(schema, model) }}

    {%- endif %}

    insert into "{{ schema }}"."{{ identifier }}" ({{ dest_cols_csv }})
    (
      select {{ dest_cols_csv }}
      from "{{ identifier }}__dbt_incremental_tmp"
    );

  {%- endif %}

{%- endmacro %}
