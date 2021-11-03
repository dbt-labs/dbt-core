{{ 
    config(materialized='table') 
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = 'varchar(10)' %}

select id
       ,cast(field1 as {{string_type}}) as field1

from source_data
order by id 