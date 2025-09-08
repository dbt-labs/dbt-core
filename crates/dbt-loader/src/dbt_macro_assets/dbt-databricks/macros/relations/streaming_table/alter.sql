{% macro get_alter_streaming_table_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    {{- log('Applying ALTER to: ' ~ relation) -}}
    {%- do return(adapter.dispatch('get_alter_streaming_table_as_sql', 'dbt')(
        relation,
        configuration_changes,
        sql,
        existing_relation,
        backup_relation,
        intermediate_relation
    )) -%}
{% endmacro %}


{% macro get_streaming_table_configuration_changes(existing_relation, new_config) -%}
    {{- log('Determining configuration changes on: ' ~ existing_relation) -}}
    {%- do return(adapter.dispatch('get_streaming_table_configuration_changes', 'dbt')(existing_relation, new_config)) -%}
{%- endmacro %}

{%- macro databricks__get_streaming_table_configuration_changes(existing_relation, new_config) -%}
    {%- set _existing_streaming_table = adapter.get_relation_config(existing_relation) -%}
    {%- set streaming_table = adapter.get_config_from_model(config.model) -%}
    {%- set _configuration_changes = streaming_table.get_changeset(_existing_streaming_table) -%}
    {% do return(_configuration_changes) %}
{%- endmacro -%}

{% macro databricks__get_alter_streaming_table_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    -- apply a full refresh immediately if needed
    {% if configuration_changes.requires_full_refresh %}
        {% do return(get_replace_sql(existing_relation, relation,  sql)) %}

    -- otherwise apply individual changes as needed
    {% else %}
        {%- set alter_statement = get_alter_st_internal(relation, configuration_changes) -%}
        {%- set create_statement = get_create_st_internal(relation, configuration_changes, sql) -%}
        {%- set return_statements = [] -%}
        {%- if create_statement -%}
            {{ return_statements.append(create_statement) }}
        {%- endif -%}
        {%- if alter_statement -%}
            {{ return_statements.append(alter_statement) }}
        {%- endif -%}
        {% do return(return_statements) %}
    {%- endif -%}
{% endmacro %}

{% macro get_create_st_internal(relation, configuration_changes, sql) %}
  {# Deviation from core: partitioned_by is used here instead of partition_by when retrieving partitioning config #}
  {%- set partition_by = configuration_changes.changes["partitioned_by"] -%}
  {%- set tblproperties = configuration_changes.changes["tblproperties"] -%}
  {%- set comment = configuration_changes.changes["comment"] -%}
  CREATE OR REFRESH STREAMING TABLE {{ relation }}
    {% if partition_by -%}
        {{ get_create_sql_partition_by(partition_by.partition_by) }}
    {%- endif %}
    {% if comment -%}
        {{ get_create_sql_comment(comment.comment) }}
    {%- endif %}
    {% if tblproperties -%}
        {{ get_create_sql_tblproperties(tblproperties.tblproperties) }}
    {%- endif %}
    AS {{ sql }}
{% endmacro %}

{% macro get_alter_st_internal(relation, configuration_changes) %}
  {%- set refresh = configuration_changes.changes["refresh"] -%}
  {%- if refresh and refresh.cron -%}
    ALTER STREAMING TABLE {{ relation }}
        {{ get_alter_sql_refresh_schedule(refresh.cron, refresh.time_zone_value, False) -}}
  {%- endif -%}
{% endmacro %}
