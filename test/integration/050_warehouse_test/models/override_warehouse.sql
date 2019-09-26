{{ config(warehouse='DBT_TEST_ALT', materialized='table') }}
select current_warehouse() as warehouse
