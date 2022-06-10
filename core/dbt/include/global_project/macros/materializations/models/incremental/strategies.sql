{% macro get_incremental_append_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_append_sql', 'dbt')(arg_dict)) }}

{% endmacro %}


{% macro default__get_incremental_append_sql(arg_dict) %}

  {% do return(get_insert_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}

{% endmacro %}


{# snowflake #}
{% macro get_incremental_delete_insert_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_delete_insert_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro default__get_incremental_delete_insert_sql(arg_dict) %}

  {% do return(get_delete_insert_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"])) %}

{% endmacro %}


{# snowflake, bigquery, spark #}
{% macro get_incremental_merge_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_merge_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro default__get_incremental_merge_sql(arg_dict) %}

  {% do return(get_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"])) %}

{% endmacro %}


{% macro get_incremental_insert_overwrite_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_insert_overwrite_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro default__get_incremental_insert_overwrite_sql(arg_dict) %}

  {% do return(get_insert_overwrite_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"], arg_dict["predicates"])) %}

{% endmacro %}


{% macro get_incremental_default_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_default_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro default__get_incremental_default_sql(arg_dict) %}

  {% do return(get_incremental_append_sql(arg_dict)) %}

{% endmacro %}


{% macro get_insert_into_sql(target_relation, temp_relation, dest_columns) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ temp_relation }}
    )

{% endmacro %}
