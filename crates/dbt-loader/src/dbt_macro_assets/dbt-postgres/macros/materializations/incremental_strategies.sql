{% macro postgres__get_incremental_default_sql(arg_dict) %}

  {% if arg_dict["unique_key"] %}
    {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
  {% else %}
    {% do return(get_incremental_append_sql(arg_dict)) %}
  {% endif %}

{% endmacro %}


{#--
  Partitioning hooks: for partition_by models, create any partitions the batch needs
  (and guard against repartitioning) before the strategy DML. The `default` strategy
  delegates to these, so it is covered too. Part of issue #679.
--#}
{% macro postgres__get_incremental_append_sql(arg_dict) %}
  {% do return(postgres__partition_ddl_for_incremental(arg_dict) ~ '\n' ~ default__get_incremental_append_sql(arg_dict)) %}
{% endmacro %}


{% macro postgres__get_incremental_delete_insert_sql(arg_dict) %}
  {% do return(postgres__partition_ddl_for_incremental(arg_dict) ~ '\n' ~ default__get_incremental_delete_insert_sql(arg_dict)) %}
{% endmacro %}


{% macro postgres__get_incremental_merge_sql(arg_dict) %}
  {% do return(postgres__partition_ddl_for_incremental(arg_dict) ~ '\n' ~ default__get_incremental_merge_sql(arg_dict)) %}
{% endmacro %}


{% macro postgres__get_incremental_microbatch_sql(arg_dict) %}

  {% if arg_dict["unique_key"] %}
    {% do return(adapter.dispatch('get_incremental_merge_sql', 'dbt')(arg_dict)) %}
  {% else %}
    {{ exceptions.raise_compiler_error("dbt-postgres 'microbatch' requires a `unique_key` config") }}
  {% endif %}

{% endmacro %}
