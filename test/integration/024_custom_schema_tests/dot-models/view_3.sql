
{{ config(materialized='table') }}


with v1 as (

    select * from {{ ref('first_schema.view_1') }}

),

v2 as (

    select * from {{ ref('second_schema.view_2') }}

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
