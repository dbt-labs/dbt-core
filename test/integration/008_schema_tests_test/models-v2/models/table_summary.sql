{{
    config(
        materialized='table'
    )
}}

select favorite_color as favorite_color_copy, count(*) as count
from {{ ref('table_copy') }}
group by 1
