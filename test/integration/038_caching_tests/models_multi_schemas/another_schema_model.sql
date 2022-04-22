{{
    config(
        materialized='table',
        schema='another_schema'
    )
}}
select 1 as id
