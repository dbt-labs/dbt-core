{{
    config(
        materialized='view',
        schema='TEST',
    )
}}

select * from {{ ref('seed') }}
