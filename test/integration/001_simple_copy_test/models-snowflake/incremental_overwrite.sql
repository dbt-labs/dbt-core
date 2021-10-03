
{{ config(materialized='incremental', unique_key='id') }}

select 1 as id
union all
select 1 as id
