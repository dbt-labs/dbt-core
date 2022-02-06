{{
    config(
        materialized='table'
    )
}}

select favorite_color as favorite_color_full_list, count(*) as count
from {{ this.schema }}.seed
group by 1

UNION ALL

select 'purple' as favorite_color_full_list, null as count