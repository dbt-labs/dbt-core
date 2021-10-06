{{ config(materialized = 'table', unlogged = True) }}

select 1 as column_a
