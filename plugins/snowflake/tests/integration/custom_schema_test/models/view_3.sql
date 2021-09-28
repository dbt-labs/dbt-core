
{{ config(schema='test', materialized='table') }}


with v1 as (

    select * from {{ ref('view_1') }}

),

v2 as (

    select * from {{ ref('view_2') }}

),

combined as (

    select last_name from v1
    union all
    select last_name from v2

)

select
    last_name,
    count(*) as count

from combined
group by 1
