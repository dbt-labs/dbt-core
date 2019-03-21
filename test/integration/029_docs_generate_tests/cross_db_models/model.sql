{{
    config(
        materialized='view',
        database=var('alternate_db')
    )
}}

select * from {{ ref('seed') }}
