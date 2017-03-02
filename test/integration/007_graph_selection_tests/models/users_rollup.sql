
{{
    config(
        materialized = 'view'
    )
}}

with users as (

    select * from {{ ref('users') }}

)

select
    gender,
    count(*) as ct
from users
group by 1
