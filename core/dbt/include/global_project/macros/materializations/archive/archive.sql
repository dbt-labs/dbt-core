{#
    Create SCD Hash SQL fields cross-db
#}

{% macro archive_hash_arguments(args) %}
  {{ adapter_macro('archive_hash_arguments', args) }}
{% endmacro %}

{% macro default__archive_hash_arguments(args) %}
    md5({% for arg in args %}coalesce(cast({{ arg }} as varchar ), '') {% if not loop.last %} || '|' || {% endif %}{% endfor %})
{% endmacro %}

{% macro create_temporary_table(sql, relation) %}
  {{ return(adapter_macro('create_temporary_table', sql, relation)) }}
{% endmacro %}

{% macro default__create_temporary_table(sql, relation) %}
    {% call statement() %}
        {{ create_table_as(True, relation, sql) }}
    {% endcall %}
    {{ return(relation) }}
{% endmacro %}

{#
    Add new columns to the table if applicable
#}
{% macro create_columns(relation, columns) %}
  {{ adapter_macro('create_columns', relation, columns) }}
{% endmacro %}

{% macro default__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation }} add column "{{ column.name }}" {{ column.data_type }};
    {% endcall %}
  {% endfor %}
{% endmacro %}

{#
    Run the update part of an archive query. Different databases have
    tricky differences in their `update` semantics. Table projection is
    not allowed on Redshift/pg, but is effectively required on bq.
#}

{% macro archive_update(target_relation, tmp_relation) %}
    {{ adapter_macro('archive_update', target_relation, tmp_relation) }}
{% endmacro %}

{% macro default__archive_update(target_relation, tmp_relation) %}
    update {{ target_relation }}
    set dbt_valid_to = tmp.dbt_valid_to
    from {{ tmp_relation }} as tmp
    where tmp.dbt_scd_id = {{ target_relation }}.dbt_scd_id
      and change_type = 'update';
{% endmacro %}


{% macro archive_get_time() -%}
  {{ adapter_macro('archive_get_time') }}
{%- endmacro %}

{% macro default__archive_get_time() -%}
  {{ current_timestamp() }}
{%- endmacro %}

{% macro snowflake__archive_get_time() -%}
  to_timestamp_ntz({{ current_timestamp() }})
{%- endmacro %}


{% macro archive_select_generic(source_sql, target_relation, transforms, scd_hash) -%}
    with source as (
      {{ source_sql }}
    ),
    {{ transforms }}
    merged as (

      select *, 'update' as change_type from updates
      union all
      select *, 'insert' as change_type from insertions

    )

    select *,
        {{ scd_hash }} as dbt_scd_id
    from merged

{%- endmacro %}

{#
    Cross-db compatible archival implementation
#}
{% macro archive_select_timestamp(source_sql, target_relation, source_columns, unique_key, updated_at) -%}
    {% set timestamp_column = api.Column.create('_', 'timestamp') %}
    {% set transforms -%}
    current_data as (

        select
            {% for col in source_columns %}
                {{ col.name }} {% if not loop.last %},{% endif %}
            {% endfor %},
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            {{ updated_at }} as dbt_valid_from,
            {{ timestamp_column.literal('null') }} as tmp_valid_to
        from source
    ),

    archived_data as (

        select
            {% for col in source_columns %}
                {{ col.name }},
            {% endfor %}
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            dbt_valid_from,
            dbt_valid_to as tmp_valid_to
        from {{ target_relation }}

    ),

    insertions as (

        select
            current_data.*,
            {{ timestamp_column.literal('null') }} as dbt_valid_to
        from current_data
        left outer join archived_data
          on archived_data.dbt_pk = current_data.dbt_pk
        where
          archived_data.dbt_pk is null
          or (
                archived_data.dbt_pk is not null
            and archived_data.dbt_updated_at < current_data.dbt_updated_at
            and archived_data.tmp_valid_to is null
        )
    ),

    updates as (

        select
            archived_data.*,
            current_data.dbt_updated_at as dbt_valid_to
        from current_data
        left outer join archived_data
          on archived_data.dbt_pk = current_data.dbt_pk
        where archived_data.dbt_pk is not null
          and archived_data.dbt_updated_at < current_data.dbt_updated_at
          and archived_data.tmp_valid_to is null
    ),
    {%- endset %}
    {%- set scd_hash = archive_hash_arguments(['dbt_pk', 'dbt_updated_at']) -%}
    {{ archive_select_generic(source_sql, target_relation, transforms, scd_hash) }}
{%- endmacro %}


{% macro archive_select_check_cols(source_sql, target_relation, source_columns, unique_key, check_cols) -%}
    {%- set timestamp_column = api.Column.create('_', 'timestamp') -%}

    {# if we recognize the primary key, it's the newest record, and anything we care about has changed, it's an update candidate #}
    {%- set update_candidate -%}
      archived_data.dbt_pk is not null
      and (
        {%- for col in check_cols %}
        current_data.{{ col }} <> archived_data.{{ col }}
        {%- if not loop.last %} or {% endif %}
      {% endfor -%}
      )
      and archived_data.tmp_valid_to is null
    {%- endset %}

    {% set transforms -%}
    current_data as (

        select
            {% for col in source_columns %}
                {{ col.name }} {% if not loop.last %},{% endif %}
            {% endfor %},
            {{ archive_get_time() }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            {{ archive_get_time() }} as dbt_valid_from,
            {{ timestamp_column.literal('null') }} as tmp_valid_to
        from source
    ),

    archived_data as (

        select
            {% for col in source_columns %}
                {{ col.name }},
            {% endfor %}
            dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            dbt_valid_from,
            dbt_valid_to as tmp_valid_to
        from {{ target_relation }}

    ),

    insertions as (

        select
            current_data.*,
            {{ timestamp_column.literal('null') }} as dbt_valid_to
        from current_data
        left outer join archived_data
          on archived_data.dbt_pk = current_data.dbt_pk
        where
          archived_data.dbt_pk is null
          or ( {{ update_candidate }} )
    ),

    updates as (

        select
            archived_data.*,
            {{ archive_get_time() }} as dbt_valid_to
        from current_data
        left outer join archived_data
          on archived_data.dbt_pk = current_data.dbt_pk
        where {{ update_candidate }}
    ),
    {%- endset %}

    {%- set hash_components = ['dbt_pk'] %}
    {%- do hash_components.extend(check_cols) -%}
    {%- set scd_hash = archive_hash_arguments(hash_components) -%}
    {{ archive_select_generic(source_sql, target_relation, transforms, scd_hash) }}
{%- endmacro %}

{# this is gross #}
{% macro create_empty_table_as(sql) %}
  {% set tmp_relation = api.Relation.create(identifier=model['name']+'_dbt_archival_view_tmp', type='view') %}
  {% set limited_sql -%}
    with cte as (
      {{ sql }}
    )
    select * from cte limit 0
  {%- endset %}
  {%- set tmp_relation = create_temporary_table(limited_sql, tmp_relation) -%}

  {{ return(tmp_relation) }}

{% endmacro %}


{% materialization archive, default %}
  {%- set config = model['config'] -%}

  {%- set target_database = config.get('target_database') -%}
  {%- set target_schema = config.get('target_schema') -%}
  {%- set target_table = model.get('alias', model.get('name')) -%}
  {%- set strategy = config.get('strategy') -%}

  {{ create_schema(target_database, target_schema) }}

  {%- set target_relation = adapter.get_relation(
      database=target_database,
      schema=target_schema,
      identifier=target_table) -%}

  {%- if target_relation is none -%}
    {%- set target_relation = api.Relation.create(
        database=target_database,
        schema=target_schema,
        identifier=target_table) -%}
  {%- elif not target_relation.is_table -%}
    {{ exceptions.relation_wrong_type(target_relation, 'table') }}
  {%- endif -%}

  {% set source_info_model = create_empty_table_as(model['injected_sql']) %}

  {%- set source_columns = adapter.get_columns_in_relation(source_info_model) -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set dest_columns = source_columns + [
      api.Column.create('dbt_valid_from', 'timestamp'),
      api.Column.create('dbt_valid_to', 'timestamp'),
      api.Column.create('dbt_scd_id', 'string'),
      api.Column.create('dbt_updated_at', 'timestamp'),
  ] -%}

  {% call statement() %}
    {{ create_archive_table(target_relation, dest_columns) }}
  {% endcall %}

  {% set missing_columns = adapter.get_missing_columns(source_info_model, target_relation) %}

  {{ create_columns(target_relation, missing_columns) }}

  {{ adapter.valid_archive_target(target_relation) }}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_archival_tmp' -%}

  {% set tmp_table_sql -%}

      with dbt_archive_sbq as (

      {% if strategy == 'timestamp' %}
        {%- set updated_at = config.get('updated_at') -%}
        {{ archive_select_timestamp(model['injected_sql'], target_relation, source_columns, unique_key, updated_at) }}
      {% elif strategy == 'check' %}
        {%- set check_cols = config.get('check_cols') -%}
        {% if check_cols == 'all' %}
          {% set check_cols = source_columns | map(attribute='name') | list %}
        {% endif %}
        {{ archive_select_check_cols(model['injected_sql'], target_relation, source_columns, unique_key, check_cols)}}
      {% else %}
        {{ exceptions.raise_compiler_error('Got invalid strategy "{}"'.format(strategy)) }}
      {% endif %}
      )
      select * from dbt_archive_sbq

  {%- endset %}

  {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier, type='table') -%}
  {%- set tmp_relation = create_temporary_table(tmp_table_sql, tmp_relation) -%}

  {{ adapter.expand_target_column_types(temp_table=tmp_identifier,
                                        to_relation=target_relation) }}

  {% call statement('_') -%}
    {{ archive_update(target_relation, tmp_relation) }}
  {% endcall %}

  {% call statement('main') -%}

    insert into {{ target_relation }} (
      {{ column_list(dest_columns) }}
    )
    select {{ column_list(dest_columns) }} from {{ tmp_relation }}
    where change_type = 'insert';
  {% endcall %}

  {{ adapter.commit() }}
{% endmaterialization %}
