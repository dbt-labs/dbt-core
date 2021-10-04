{{ config(materialized='table') }}
 
with source_data as (

    {#- This is model three #}
 
    select 1 as id
    union all
    select null as id
 
)
 
select *
from source_data
