{% macro snowflake__get_create_dynamic_table_as_sql(relation, sql) -%}
{#-
--  Produce DDL that creates a dynamic table
--
--  Args:
--  - relation: Union[SnowflakeRelation, str]
--      - SnowflakeRelation - required for relation.render()
--      - str - is already the rendered relation name
--  - sql: str - the code defining the model
--  Globals:
--  - config: NodeConfig - contains the attribution required to produce a SnowflakeDynamicTableConfig
--  Returns:
--      A valid DDL statement which will result in a new dynamic table.
-#}

    {%- set dynamic_table = relation.from_config(config.model) -%}

    {%- if dynamic_table.catalog.table_format == 'iceberg' -%}
        {{ _get_create_dynamic_iceberg_table_as_sql(dynamic_table, relation, sql) }}
    {%- else -%}
        {{ _get_create_dynamic_standard_table_as_sql(dynamic_table, relation, sql) }}
    {%- endif -%}

{%- endmacro %}


{% macro _get_create_dynamic_standard_table_as_sql(dynamic_table, relation, sql) -%}
{#-
--  Produce DDL that creates a standard dynamic table
--
--  This follows the syntax outlined here:
--  https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#syntax
--
--  Args:
--  - dynamic_table: SnowflakeDynamicTableConfig - contains all of the configuration for the dynamic table
--  - relation: Union[SnowflakeRelation, str]
--      - SnowflakeRelation - required for relation.render()
--      - str - is already the rendered relation name
--  - sql: str - the code defining the model
--  Returns:
--      A valid DDL statement which will result in a new dynamic standard table.
-#}

    create dynamic table {{ relation }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.snowflake_warehouse }}
        {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
        {{ optional('initialize', dynamic_table.initialize) }}
        as (
            {{ sql }}
        )

{%- endmacro %}


{% macro _get_create_dynamic_iceberg_table_as_sql(dynamic_table, relation, sql) -%}
{#-
--  Produce DDL that creates a dynamic iceberg table
--
--  This follows the syntax outlined here:
--  https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#create-dynamic-iceberg-table
--
--  Args:
--  - dynamic_table: SnowflakeDynamicTableConfig - contains all of the configuration for the dynamic table
--  - relation: Union[SnowflakeRelation, str]
--      - SnowflakeRelation - required for relation.render()
--      - str - is already the rendered relation name
--  - sql: str - the code defining the model
--  Returns:
--      A valid DDL statement which will result in a new dynamic iceberg table.
-#}

    create dynamic iceberg table {{ relation }}
        target_lag = '{{ dynamic_table.target_lag }}'
        warehouse = {{ dynamic_table.snowflake_warehouse }}
        {{ optional('external_volume', dynamic_table.catalog.external_volume) }}
        {{ optional('catalog', dynamic_table.catalog.name) }}
        base_location = '{{ dynamic_table.catalog.base_location }}'
        {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
        {{ optional('initialize', dynamic_table.initialize) }}
        as (
            {{ sql }}
        )

{%- endmacro %}
