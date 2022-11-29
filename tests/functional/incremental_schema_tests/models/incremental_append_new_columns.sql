{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% set string_type = 'varchar(10)' %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id, 
       cast(field1 as {{string_type}}) as field1, 
       cast(field2 as {{string_type}}) as field2, 
       cast(field3 as {{string_type}}) as field3, 
       cast(field4 as {{string_type}}) as field4 
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id, 
       cast(field1 as {{string_type}}) as field1, 
       cast(field2 as {{string_type}}) as field2 
FROM source_data where id <= 3

{% endif %}