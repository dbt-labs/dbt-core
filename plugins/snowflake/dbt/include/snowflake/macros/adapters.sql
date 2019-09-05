{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {%- set transient = config.get('transient', default=true) -%}
  {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
  {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
  {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
    {%- set cluster_by_keys = [cluster_by_keys] -%}
  {%- endif -%}
  {%- if cluster_by_keys is not none -%}
    {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
  {% else %}
    {%- set cluster_by_string = none -%}
  {%- endif -%}

      create or replace {% if temporary -%}
        temporary
      {%- elif transient -%}
        transient
      {%- endif %} table {{ relation }}
      as (
        {%- if cluster_by_string is not none -%}
          select * from(
            {{ sql }}
            ) order by ({{ cluster_by_string }})
        {%- else -%}
          {{ sql }}
        {%- endif %}
      );
    {% if cluster_by_string is not none and not temporary -%}
      alter table {{relation}} cluster by ({{cluster_by_string}});
    {%- endif -%}
    {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
      alter table {{relation}} resume recluster;
    {%- endif -%}

{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from
      {{ relation.information_schema('columns') }}

      where table_name ilike '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema ilike '{{ relation.schema }}'
        {% endif %}
        {% if relation.database %}
        and table_catalog ilike '{{ relation.database }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}

{% endmacro %}


{% macro snowflake__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           when table_type = 'MATERIALIZED VIEW' then 'materializedview'
           when table_type = 'EXTERNAL TABLE' then 'externaltable'
           else table_type
      end as table_type
    from {{ information_schema }}.tables
    where table_schema ilike '{{ schema }}'
      and table_catalog ilike '{{ information_schema.database.lower() }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro snowflake__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
        select count(*)
        from {{ information_schema }}.schemata
        where upper(schema_name) = upper('{{ schema }}')
            and upper(catalog_name) = upper('{{ information_schema.database }}')
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{%- endmacro %}

{% macro snowflake__current_timestamp() -%}
  convert_timezone('UTC', current_timestamp())
{%- endmacro %}

{% macro snowflake__snapshot_get_time() -%}
  to_timestamp_ntz({{ current_timestamp() }})
{%- endmacro %}


{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro snowflake__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter {{ column_name }} set data type {{ new_column_type }};
  {% endcall %}
{% endmacro %}
