{{ config(materialized='table') }}
select '{{ env_var("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TEST_ALT") }}' as warehouse
