{{ config(materialized='ephemeral') }}

select * from {{ ref('base_copy') }} where gender = 'Female'
