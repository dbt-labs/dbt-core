{% macro bq_generate_incremental_insert_overwrite_build_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
) %}
    {% if partition_by is none %}
      {% set missing_partition_msg -%}
      The 'insert_overwrite' strategy requires the `partition_by` config.
      {%- endset %}
      {% do exceptions.raise_compiler_error(missing_partition_msg) %}
    {% endif %}

    {% set build_sql = bq_insert_overwrite_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
    ) %}

    {{ return(build_sql) }}

{% endmacro %}

{% macro bq_copy_partitions(tmp_relation, target_relation, partitions, partition_by) %}

  {% for partition in partitions %}
    {% if partition_by.data_type == 'int64' %}
      {% set partition = partition | as_text %}
    {% elif partition_by.granularity == 'hour' %}
      {% set partition = partition.strftime("%Y%m%d%H") %}
    {% elif partition_by.granularity == 'day' %}
      {% set partition = partition.strftime("%Y%m%d") %}
    {% elif partition_by.granularity == 'month' %}
      {% set partition = partition.strftime("%Y%m") %}
    {% elif partition_by.granularity == 'year' %}
      {% set partition = partition.strftime("%Y") %}
    {% endif %}
    {% set tmp_relation_partitioned = api.Relation.create(database=tmp_relation.database, schema=tmp_relation.schema, identifier=tmp_relation.table ~ '$' ~ partition, type=tmp_relation.type) %}
    {% set target_relation_partitioned = api.Relation.create(database=target_relation.database, schema=target_relation.schema, identifier=target_relation.table ~ '$' ~ partition, type=target_relation.type) %}
    {% do adapter.copy_table(tmp_relation_partitioned, target_relation_partitioned, "table") %}
  {% endfor %}

{% endmacro %}

{% macro bq_insert_overwrite_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
) %}
  {% if partitions is not none and partitions != [] %} {# static #}
      {{ bq_static_insert_overwrite_sql(tmp_relation, target_relation, sql, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions) }}
  {% else %} {# dynamic #}
      {{ bq_dynamic_insert_overwrite_sql(tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, copy_partitions) }}
  {% endif %}
{% endmacro %}

{% macro bq_static_insert_overwrite_sql(
    tmp_relation, target_relation, sql, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
) %}

      {% set predicate -%}
          {{ partition_by.render_wrapped(alias='DBT_INTERNAL_DEST') }} in (
              {{ partitions | join (', ') }}
          )
      {%- endset %}

      {%- set source_sql -%}
        (
          {% if partition_by.time_ingestion_partitioning and tmp_relation_exists -%}
          select
            {{ partition_by.insertable_time_partitioning_field() }},
            * from {{ tmp_relation }}
          {% elif tmp_relation_exists -%}
            select
            * from {{ tmp_relation }}
          {%- elif partition_by.time_ingestion_partitioning -%}
            {{ wrap_with_time_ingestion_partitioning_sql(partition_by, sql, True) }}
          {%- else -%}
            {{sql}}
          {%- endif %}

        )
      {%- endset -%}

      {% if copy_partitions %}
          {% do bq_copy_partitions(tmp_relation, target_relation, partitions, partition_by) %}
      {% else %}

      {#-- In case we're putting the model SQL _directly_ into the MERGE statement,
         we need to prepend the MERGE statement with the user-configured sql_header,
         which may be needed to resolve that model SQL (e.g. referencing a variable or UDF in the header)
         in the "temporary table exists" case, we save the model SQL result as a temp table first, wherein the
         sql_header is included by the create_table_as macro.
      #}
      -- 1. run the merge statement
      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], include_sql_header = not tmp_relation_exists) }};

      {%- if tmp_relation_exists -%}
      -- 2. clean up the temp table
      drop table if exists {{ tmp_relation }};
      {%- endif -%}

  {% endif %}
{% endmacro %}

{% macro bq_dynamic_copy_partitions_insert_overwrite_sql(
  tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, copy_partitions
  ) %}
  {%- if tmp_relation_exists is false -%}
  {# We run temp table creation in a separated script to move to partitions copy if it doesn't already exist #}
    {%- call statement('create_tmp_relation_for_copy', language='sql') -%}
      {{ bq_create_table_as(partition_by, True, tmp_relation, sql, 'sql')
    }}
    {%- endcall %}
  {%- endif -%}
  {%- set partitions_sql -%}
    select distinct {{ partition_by.render_wrapped() }}
    from {{ tmp_relation }}
  {%- endset -%}
  {%- set partitions = run_query(partitions_sql).columns[0].values() -%}
  {# We copy the partitions #}
  {%- do bq_copy_partitions(tmp_relation, target_relation, partitions, partition_by) -%}
  -- Clean up the temp table
  drop table if exists {{ tmp_relation }}
{% endmacro %}

{% macro bq_dynamic_insert_overwrite_sql(tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, copy_partitions) %}
  {%- if copy_partitions is true %}
     {{ bq_dynamic_copy_partitions_insert_overwrite_sql(tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, copy_partitions) }}
  {% else -%}
      {% set predicate -%}
          {{ partition_by.render_wrapped(alias='DBT_INTERNAL_DEST') }} in unnest(dbt_partitions_for_replacement)
      {%- endset %}

      {%- set source_sql -%}
      (
        select
        {% if partition_by.time_ingestion_partitioning -%}
        {{ partition_by.insertable_time_partitioning_field() }},
        {%- endif -%}
        * from {{ tmp_relation }}
      )
      {%- endset -%}

      -- generated script to merge partitions into {{ target_relation }}
      declare dbt_partitions_for_replacement array<{{ partition_by.data_type_for_partition() }}>;

      {# have we already created the temp table to check for schema changes? #}
      {% if not tmp_relation_exists %}
       -- 1. create a temp table with model data
        {{ bq_create_table_as(partition_by, True, tmp_relation, sql, 'sql') }}
      {% else %}
        -- 1. temp table already exists, we used it to check for schema changes
      {% endif %}
      {%- set partition_field = partition_by.time_partitioning_field() if partition_by.time_ingestion_partitioning else partition_by.render_wrapped() -%}

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
              array_agg(distinct {{ partition_field }} IGNORE NULLS)
          from {{ tmp_relation }}
      );

      -- 3. run the merge statement
      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate]) }};

      -- 4. clean up the temp table
      drop table if exists {{ tmp_relation }}

  {% endif %}

{% endmacro %}
