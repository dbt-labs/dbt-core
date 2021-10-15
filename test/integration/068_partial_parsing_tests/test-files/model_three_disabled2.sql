- Disabled model
{{ config(materialized='table', enabled=False) }}
 
with source_data as (
 
    select 1 as id
    union all
    select null as id
 
)
 
select *
from source_data
