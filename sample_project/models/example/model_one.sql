{{
    config(
        materialized='table',
        alias='model_one',
        tags=["im_code"]
    )
}}

with source_data as (

    select 1 as id
    union all
    select 2 as id

)

select *
from source_data
