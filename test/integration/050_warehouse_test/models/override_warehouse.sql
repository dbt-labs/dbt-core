{{ config(snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TEST_ALT'), materialized='table') }}
select current_warehouse() as warehouse
