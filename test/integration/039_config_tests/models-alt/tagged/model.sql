{{
    config(
        materialized='view',
        tags=['tag_1_in_model'],
    )
}}

{{
    config(
        materialized='table',
        tags=['tag_2_in_model'],
    )
}}

select 4 as id, 2 as value
