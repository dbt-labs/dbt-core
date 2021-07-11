{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
        
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = 'string' if target.type == 'bigquery' else 'varchar(10)' %}

{% if is_incremental()  %}

SELECT id, field1, cast(field3 as {{string_type}}) as field3, field4 FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id, field1, field2 FROm source_data LIMIT 3

{% endif %}