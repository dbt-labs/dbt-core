{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
        
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = 'string' if target.type == 'bigquery' else 'varchar(10)' %}

{% if is_incremental() %}

SELECT id, 
       cast(field1 as {{string_type}}) as field1, -- to validate type changes on existing column
       cast(field3 as {{string_type}}) as field3, -- to validate new fields
       field4 -- to validate new fields
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id, field1, field2 FROM source_data WHERE id <= 3

{% endif %}