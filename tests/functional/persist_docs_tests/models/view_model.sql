{{ config(materialized='view') }}
select 2 as id, 'Bob' as name
