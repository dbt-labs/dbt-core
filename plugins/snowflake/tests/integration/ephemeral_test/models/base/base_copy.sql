{{ config(materialized='ephemeral') }}

select * from {{ ref('base') }}
