
{{ config(materialized='incremental', sql_where=True, unique_key='id') }}

-- incremental model
select 1 as id
