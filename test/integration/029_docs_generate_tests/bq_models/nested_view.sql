{{
    config(
        materialized='view'
    )
}}

select * from {{ ref('nested_table') }}
