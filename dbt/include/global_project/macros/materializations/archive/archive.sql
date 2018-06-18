{% macro archive_select(source_relation, target_relation, unique_key, updated_at) %}

    with current_data as (

        select
            {% for col in adapter.get_columns_in_table(source_relation.schema, source_relation.identifier) %}
                "{{ col.name }}" {% if not loop.last %},{% endif %}
            {% endfor %},
            {{ updated_at }} as "dbt_updated_at",
            {{ unique_key }} as "dbt_pk",
            {{ updated_at }} as "valid_from",
            null::timestamp as "tmp_valid_to"
        from {{ source_relation }}

    ),

    archived_data as (

        select
            {% for col in adapter.get_columns_in_table(source_relation.schema, source_relation.identifier) %}
                "{{ col.name }}" {% if not loop.last %},{% endif %}
            {% endfor %},
            {{ updated_at }} as "dbt_updated_at",
            {{ unique_key }} as "dbt_pk",
            "valid_from",
            "valid_to" as "tmp_valid_to"
        from {{ target_relation }}

    ),

    insertions as (

        select
            current_data.*,
            null::timestamp as "valid_to"
        from current_data
        left outer join archived_data
          on archived_data."dbt_pk" = current_data."dbt_pk"
        where archived_data."dbt_pk" is null or (
          archived_data."dbt_pk" is not null and
          current_data."dbt_updated_at" > archived_data."dbt_updated_at" and
          archived_data."tmp_valid_to" is null
        )
    ),

    updates as (

        select
            archived_data.*,
            current_data."dbt_updated_at" as "valid_to"
        from current_data
        left outer join archived_data
          on archived_data."dbt_pk" = current_data."dbt_pk"
        where archived_data."dbt_pk" is not null
          and archived_data."dbt_updated_at" < current_data."dbt_updated_at"
          and archived_data."tmp_valid_to" is null
    ),

    merged as (

      select *, 'update' as "change_type" from updates
      union all
      select *, 'insert' as "change_type" from insertions

    )

    select *,
        md5("dbt_pk" || '|' || "dbt_updated_at") as "scd_id"
    from merged

{% endmacro %}

{% materialization archive, default %}
  {%- set config = model['config'] -%}

  {%- set target_schema = config.get('target_schema') -%}
  {%- set target_table = config.get('target_table') -%}

  {%- set source_schema = config.get('source_schema') -%}
  {%- set source_table = config.get('source_table') -%}

  {%- set source_relation = adapter.get_relation(
      schema=source_schema,
      identifier=source_table) -%}

  {%- set target_relation = adapter.get_relation(
      schema=target_schema,
      identifier=target_table) -%}

  {%- if source_relation is none -%}
    {{ exceptions.missing_relation(source_relation) }}
  {%- endif -%}

  {%- if target_relation is none -%}
    {%- set target_relation = api.Relation.create(
        schema=target_schema,
        identifier=target_table) -%}
  {%- elif not target_relation.is_table -%}
    {{ exceptions.relation_wrong_type(target_relation, 'table') }}
  {%- endif -%}

  {%- set source_columns = adapter.get_columns_in_table(source_schema, source_table) -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set updated_at = config.get('updated_at') -%}
  {%- set dest_columns = source_columns + [
      column('valid_from', 'timestamp', None),
      column('valid_to', 'timestamp', None),
      column('scd_id', 'text', None),
      column('dbt_updated_at', 'timestamp', None)
  ] -%}

  {% call statement() %}
    {{ create_schema(target_schema) }}
  {% endcall %}

  {% call statement() %}
    {{ create_archive_table(target_relation, dest_columns) }}
  {% endcall %}

  {% set missing_columns = adapter.get_missing_columns(source_schema, source_table, target_schema, target_table) %}
  {% set dest_columns = adapter.get_columns_in_table(target_schema, target_table) + missing_columns %}

  {% for col in missing_columns %}
    {% call statement() %}
      alter table {{ target_relation }}
      add column "{{ col.name }}" {{ col.data_type }};
    {% endcall %}
  {% endfor %}

  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_archival_tmp' -%}
  {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier, type='table') -%}

  {% call statement() %}
    {% set tmp_table_sql -%}

      with dbt_archive_sbq as (
        {{ archive_select(source_relation, target_relation, unique_key, updated_at) }}
      )
      select * from dbt_archive_sbq

    {%- endset %}

    {{ dbt.create_table_as(True, tmp_relation, tmp_table_sql) }}

  {% endcall %}

  {{ adapter.expand_target_column_types(temp_table=tmp_identifier,
                                        to_schema=target_schema,
                                        to_table=target_table) }}

  {% call statement('main') -%}
    update {{ target_relation }} set "valid_to" = tmp."valid_to"
    from {{ tmp_relation }} as tmp
    where tmp."scd_id" = {{ target_relation }}."scd_id"
      and "change_type" = 'update';

    insert into {{ target_relation }} (
      {{ column_list(dest_columns) }}
    )
    select {{ column_list(dest_columns) }} from {{ tmp_relation }}
    where "change_type" = 'insert';
  {% endcall %}

  {{ adapter.commit() }}
{% endmaterialization %}
