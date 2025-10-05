-- funcsign: (relation, string) -> string
{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    {%- set dynamic_table = relation.from_config(config.model) -%}

    {%- if catalog_relation.catalog_type == 'INFO_SCHEMA' -%}
        {{ snowflake__create_dynamic_table_info_schema_sql(dynamic_table, relation, compiled_code) }}
    {%- elif catalog_relation.catalog_type == 'BUILT_IN' -%}
        {{ snowflake__create_dynamic_table_built_in_sql(dynamic_table, relation, compiled_code) }}
    {%- else -%}
        {% do exceptions.raise_compiler_error('Unexpected model config for: ' ~ relation) %}
    {%- endif -%}

{%- endmacro %}


{% macro snowflake__create_dynamic_table_info_schema_sql(dynamic_table, relation, sql) -%}
{#-
    Produce DDL that creates an info schema dynamic table

    Implements CREATE DYNAMIC TABLE:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#syntax

    Args:
    - dynamic_table: SnowflakeDynamicTableConfig - contains all of the configuration for the dynamic table
    - relation: Union[SnowflakeRelation, str]
        - SnowflakeRelation - required for relation.render()
        - str - is already the rendered relation name
    - sql: str - the code defining the model
    Returns:
        A valid DDL statement which will result in a new dynamic info schema table.
-#}

    create dynamic table {{ relation }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.snowflake_warehouse }}
        {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
        {{ optional('initialize', dynamic_table.initialize) }}
        {{ optional('with row access policy', dynamic_table.row_access_policy, equals_char='') }}
        {{ optional('with tag', dynamic_table.table_tag, quote_char='(', equals_char='') }}
        as (
            {{ sql }}
        )

{%- endmacro %}


{% macro snowflake__create_dynamic_table_built_in_sql(dynamic_table, relation, sql) -%}
{#-
    Produce DDL that creates a dynamic iceberg table

    Implements CREATE DYNAMIC ICEBERG TABLE (Snowflake as the Iceberg catalog):
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#create-dynamic-iceberg-table

    Args:
    - dynamic_table: SnowflakeDynamicTableConfig - contains all of the configuration for the dynamic table
    - relation: Union[SnowflakeRelation, str]
        - SnowflakeRelation - required for relation.render()
        - str - is already the rendered relation name
    - sql: str - the code defining the model
    Returns:
        A valid DDL statement which will result in a new dynamic iceberg table.
-#}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    create dynamic iceberg table {{ relation }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.snowflake_warehouse }}
        {{ optional('external_volume', catalog_relation.external_volume, "'") }}
        catalog = 'SNOWFLAKE'  -- required, and always SNOWFLAKE for built-in Iceberg tables
        base_location = '{{ catalog_relation.base_location }}'
        {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
        {{ optional('initialize', dynamic_table.initialize) }}
        {{ optional('row_access_policy', dynamic_table.row_access_policy) }}
        {{ optional('table_tag', dynamic_table.table_tag) }}
        as (
            {{ sql }}
        )

{%- endmacro %}
