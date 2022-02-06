{{
    config(materialized='table')
}}

select id, value from {{ source('raw', 'some_seed') }}
