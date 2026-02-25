{% macro get_create_streaming_table_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_streaming_table_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro databricks__get_create_streaming_table_as_sql(relation, sql) -%}
  {%- set streaming_table = adapter.get_config_from_model(config.model) -%}
  {%- set partition_by = streaming_table.config["partition_by"].partition_by -%}
  {%- set tblproperties = streaming_table.config["tblproperties"].tblproperties -%}
  {%- set comment = streaming_table.config["comment"].comment -%}
  {%- set refresh = streaming_table.config["refresh"] -%}

  {%- set analysis_sql = sql | replace('STREAM ', '') | replace('stream ', '') -%}

  {#
    TODO: When DESCRIBE QUERY EXTENDED is supported, this implementation should be simplified
    to use that instead. For now, we work around this limitation by writing results to a
    temporary view and using DESCRIBE TABLE EXTENDED on the temporary view.
  #}
  {%- set temp_relation = make_temp_relation(relation) -%}
  {% call statement('create_temp_view') -%}
    {%- set view_sql = analysis_sql.rstrip('; \n\t') -%}
    {{ create_temporary_view(temp_relation, view_sql) }}
  {%- endcall %}

  {%- set columns = adapter.get_columns_in_relation(temp_relation) -%}
  {%- set model_columns = model.get('columns', {}) -%}
  {%- set columns_and_constraints = adapter.parse_columns_and_constraints(columns, model_columns, []) -%}

  {#-- We don't enrich the relation with model constraints because they are not supported for streaming tables --#}
  CREATE STREAMING TABLE {{ relation.render() }}
    {{ get_column_and_constraints_sql(relation, columns_and_constraints[0]) }}
    {{ get_create_sql_partition_by(partition_by) }}
    {{ liquid_clustered_cols() }}
    {{ get_create_sql_comment(comment) }}
    {{ get_create_sql_tblproperties(tblproperties) }}
    {{ get_create_sql_refresh_schedule(refresh.cron, refresh.time_zone_value) }}
    AS {{ sql }}
{% endmacro %}
