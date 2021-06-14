{{ 
    config(materialized='table') 
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,field1
       -- ,field2
       ,CASE WHEN id <= 3 THEN NULL ELSE field3 END AS field3
       ,CASE WHEN id <= 3 THEN NULL ELSE field4 END AS field4

from source_data