{{
    config(
        materialized='table',
        alias='model_two',
        tags=["im_model","im_generic"]
    )
}}

select *
from {{ ref('model_one') }}
where id = 1
