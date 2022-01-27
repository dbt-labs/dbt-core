{{
    config(
        materialized='view',
        tags=['tag_two'],
    )
}}

{{
    config(
        materialized='table',
        tags=['tag_three'],
    )
}}

select 4 as id, 2 as value
