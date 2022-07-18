{{ config(materialized='ephemeral') }}

select * from {{ ref('first_schema.view_1') }}
