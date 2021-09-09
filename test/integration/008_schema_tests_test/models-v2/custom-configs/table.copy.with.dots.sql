
{{
    config(
        materialized='table',
        alias='table_copy_with_dots'
    )
}}

select * from {{ this.schema }}.seed
