{{
    config(
        materialized='view',
        schema='test',
    )
}}

select * from {{ ref('seed') }}
