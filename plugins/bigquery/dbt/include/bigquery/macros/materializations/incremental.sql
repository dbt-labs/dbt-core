
{% macro bq_partition_merge(tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns) %}
  {%- set array_datatype = 
      'date' if partition_by.data_type in ('timestamp, datetime') 
      else partition_by.data_type -%}

  {% set predicate -%}
      {{ pprint_partition_field(
          partition_by,
          alias = 'DBT_INTERNAL_DEST')
      }} in unnest(partitions_for_upsert)
  {%- endset %}

  {%- set source_sql -%}
  (
    select * from {{tmp_relation.identifier}}
  )
  {%- endset -%}

  -- generated script to merge partitions into {{ target_relation }}
  declare partitions_for_upsert array<{{array_datatype}}>;

  -- 1. create a temp table
  {{ create_table_as(True, tmp_relation, sql) }}

  -- 2. define partitions to update
  set (partitions_for_upsert) = (
      select as struct
          array_agg(distinct {{pprint_partition_field(partition_by)}})
      from {{tmp_relation.identifier}}
  );

  -- 3. run the merge statement
  {{ get_merge_sql(target_relation, source_sql, unique_key, dest_columns, [predicate]) }}

{% endmacro %}


{% materialization incremental, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
  {%- set cluster_by = config.get('cluster_by', none) -%}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
     {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}

     {#-- if partitioned, use BQ scripting to get the range of partition values to be updated --#}
     {% if partition_by %}
        {% set build_sql = bq_partition_merge(
            tmp_relation,
            target_relation,
            sql,
            unique_key,
            partition_by,
            dest_columns) %}

     {% else %}
       {#-- wrap sql in parens to make it a subquery --#}
       {%- set source_sql -%}
         (
           {{sql}}
         )
       {%- endset -%}

       {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns) %}

     {% endif %}

  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
