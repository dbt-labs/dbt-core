{{
    config(
        materialized='table'
    )
}}

select favorite_color, count(*) as count
from {{ ref('table_copy') }}
group by 1
