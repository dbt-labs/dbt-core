{% macro databricks__get_create_materialized_view_as_sql(relation, sql) -%}
  {%- set materialized_view = adapter.get_config_from_model(config.model) -%}
  {# Deviation from core: partitioned_by is used here instead of partition_by when retrieving partitioning config #}
  {%- set partition_by = materialized_view["partitioned_by"].partition_by -%}
  {%- set tblproperties = materialized_view["tblproperties"].tblproperties -%}
  {%- set comment = materialized_view["comment"].comment -%}
  {%- set refresh = materialized_view["refresh"] -%}
  create materialized view {{ relation }}
    {{ get_create_sql_partition_by(partition_by) }}
    {{ get_create_sql_comment(comment) }}
    {{ get_create_sql_tblproperties(tblproperties) }}
    {{ get_create_sql_refresh_schedule(refresh.cron, refresh.time_zone_value) }}
  as
    {{ sql }}
{% endmacro %}
