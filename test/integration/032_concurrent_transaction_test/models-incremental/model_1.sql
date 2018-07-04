
{{ config(materialized='incremental', sql_where=True, unique_key='id') }}

select 1 as id
