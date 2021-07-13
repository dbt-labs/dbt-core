{{ 
    config(materialized='table') 
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = 'string' if target.type == 'bigquery' else 'varchar(10)' %}

select id
       ,CAST(field1 as {{string_type}}) AS field1
       --,field2
       ,CAST(CASE WHEN id <= 3 THEN NULL ELSE field3 END AS {{string_type}}) AS field3
       ,CASE WHEN id <= 3 THEN NULL ELSE field4 END AS field4

from source_data