{% macro get_create_streaming_table_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_streaming_table_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{# Deviation from core: We return the config directly, so we no longer access streaming_table.config. #}
{% macro databricks__get_create_streaming_table_as_sql(relation, sql) -%}
  {%- set streaming_table = adapter.get_config_from_model(config.model) -%}
  {# Deviation from core: partitioned_by is used here instead of partition_by when retrieving partitioning config #}
  {%- set partition_by = streaming_table["partitioned_by"].partition_by -%}
  {%- set tblproperties = streaming_table["tblproperties"].tblproperties -%}
  {%- set comment = streaming_table["comment"].comment -%}
  {%- set refresh = streaming_table["refresh"] -%}

  CREATE STREAMING TABLE {{ relation }}
    {{ get_create_sql_partition_by(partition_by) }}
    {{ get_create_sql_comment(comment) }}
    {{ get_create_sql_tblproperties(tblproperties) }}
    {{ get_create_sql_refresh_schedule(refresh.cron, refresh.time_zone_value) }}
    AS {{ sql }}
{% endmacro %}
