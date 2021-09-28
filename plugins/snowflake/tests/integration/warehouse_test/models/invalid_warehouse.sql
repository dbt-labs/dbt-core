{{ config(snowflake_warehouse='DBT_TEST_DOES_NOT_EXIST') }}
select current_warehouse() as warehouse
