{% macro get_create_streaming_table_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_streaming_table_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro databricks__get_create_streaming_table_as_sql(relation, sql) -%}
  {%- set streaming_table = adapter.get_config_from_model(config.model) -%}
  {%- set partition_by = streaming_table.config["partition_by"].partition_by -%}
  {%- set tblproperties = streaming_table.config["tblproperties"].tblproperties -%}
  {%- set comment = streaming_table.config["comment"].comment -%}
  {%- set refresh = streaming_table.config["refresh"] -%}

  CREATE STREAMING TABLE {{ relation }}
    {{ get_create_sql_partition_by(partition_by) }}
    {{ get_create_sql_comment(comment) }}
    {{ get_create_sql_tblproperties(tblproperties) }}
    {{ get_create_sql_refresh_schedule(refresh.cron, refresh.time_zone_value) }}
    AS {{ sql }}
{% endmacro %}
