{{ 
    config(materialized='table') 
}}

{% set string_type = 'varchar(10)' %}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,cast(field1 as {{string_type}}) as field1
       ,cast(field2 as {{string_type}}) as field2
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as {{string_type}}) AS field3
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as {{string_type}}) AS field4

from source_data