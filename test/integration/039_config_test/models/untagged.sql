{{
    config(materialized='table')
}}

select id, value from {{ source('raw', 'seed') }}
