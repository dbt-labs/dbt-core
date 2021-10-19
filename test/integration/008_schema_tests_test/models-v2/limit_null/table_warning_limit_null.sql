{{
    config(
        materialized='table'
    )
}}

select * from {{ref('table_limit_null')}}