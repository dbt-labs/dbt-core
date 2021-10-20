{{
    config(
        materialized='table'
    )
}}

select * from {{ref('table_limit_null')}}

UNION ALL

select 'magenta' as favorite_color_full_list, null as count