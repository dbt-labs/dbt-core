{% macro dbt__incremental_delete(schema, model) -%}

  {%- set unique_key = config.require('unique_key') -%}
  {%- set identifier = model['name'] -%}

  delete
  from "{{ schema }}"."{{ identifier }}"
  where ({{ unique_key }}) in (
    select ({{ unique_key }})
    from "{{ identifier }}__dbt_incremental_tmp"
  );

{%- endmacro %}

{% macro dbt__incremental_insert(schema, identifier, dest_cols_csv) %}

   insert into "{{ schema }}"."{{ identifier }}" ({{ dest_cols_csv }})
   (
     select {{ dest_cols_csv }}
     from "{{ identifier }}__dbt_incremental_tmp"
   );

{% endmacro %}

{% macro debug_state(is_full_refresh, is_non_destructive, existing_type) -%}
    ERROR - unexpected incremental state
    Full Refresh? {{ is_full_refresh }}
    Non Destructive {{ is_non_destructive }}
    Existing Type? {{ existing_type }}
{%- endmacro %}

{% macro debug(msg) %}
  {{ log("         +-> " ~ msg, info=var('verbose', False)) }}
{% endmacro %}

{% macro handle_state(is_full_refresh, is_non_destructive, existing_type, schema, identifier) %}
  {% if existing_type is none %}
    -- no-op
  {% elif existing_type == 'view' %}
    {{ debug('Dropping view: ' ~ schema ~ '.' ~ identifier) }}
    {{ adapter.drop(schema, identifier, 'view') }}
  {% elif is_full_refresh and is_non_destructive %}
    {{ debug('Truncating table: ' ~ schema ~ '.' ~ identifier) }}
    {{ adapter.truncate(schema, identifier) }}
  {% elif is_full_refresh %}
    {{ debug('Dropping table: ' ~ schema ~ '.' ~ identifier) }}
    {{ adapter.drop(schema, identifier, 'table') }}
  {% elif is_non_destructive %}
    -- no-op
  {% elif not is_non_destructive and not is_full_refresh %}
    -- no-op
  {% else %}
    -- this shouldn't happen
    {{ debug(debug_state(is_full_refresh, is_non_destructive, existing_type)) }}
    {{ exceptions.raise_compiler_error("Unexpected incremental state") }}
  {% endif %}
{% endmacro %}

{% materialization incremental, default -%}
  {%- set sql_where = config.require('sql_where') -%}
  {%- set unique_key = config.get('unique_key') -%}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_incremental_tmp' -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- BEGIN happens here
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if (flags.FULL_REFRESH and not flags.NON_DESTRUCTIVE) or existing_type in (none, 'view') -%}
    {%- call statement('main') -%}
      {{ debug('Building new table...') }}
      {{ create_table_as(False, tmp_identifier, sql) }}
    {%- endcall -%}

    {{ handle_state(flags.FULL_REFRESH, flags.NON_DESTRUCTIVE, existing_type, schema, identifier) }}

    {{ debug('Renaming `' ~ tmp_identifier ~ '` to `' ~ identifier ~ '`') }}
    {{ adapter.rename(schema, tmp_identifier, identifier) }}
  {%- else -%}
    -- this will either truncate (if full-refresh && non-destructive), or no-op
    -- note: a truncate here will end the open transaction
    {{ handle_state(flags.FULL_REFRESH, flags.NON_DESTRUCTIVE, existing_type, schema, identifier) }}

    {{ debug('Creating temp table') }}
    {%- call statement() -%}
      create temporary table "{{ tmp_identifier }}" as (
        with dbt_incr_sbq as (
          {{ sql }}
        )
        select * from dbt_incr_sbq
        where ({{ sql_where }})
          or ({{ sql_where }}) is null
        );
     {%- endcall -%}

     {{ debug('Expanding column types if required') }}
     {{ adapter.expand_target_column_types(temp_table=tmp_identifier,
                                           to_schema=schema,
                                           to_table=identifier) }}

     {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
     {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

     {%- call statement(auto_begin=False) -%}
       {% if unique_key is not none -%}
         {{ debug('Deleting duplicated records') }}
         {{ dbt__incremental_delete(schema, model) }}
       {%- endif %}
     {% endcall %}

     {%- call statement('main', auto_begin=False) -%}
       {{ debug('Inserting new records') }}
       {{ dbt__incremental_insert(schema, identifier, dest_cols_csv) }}
     {%- endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization %}
