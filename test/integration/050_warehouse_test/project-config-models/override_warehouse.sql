{{ config(materialized='table') }}
select current_warehouse() as warehouse
