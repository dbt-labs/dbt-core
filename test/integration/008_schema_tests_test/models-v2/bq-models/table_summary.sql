{{
    config(
        materialized='table'
    )
}}

select favorite_color as favorite_color_copy, count(*) as count
from {{ ref('ephemeral_copy') }}
group by 1
