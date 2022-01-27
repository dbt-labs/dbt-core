{{ config(materialized='ephemeral') }}
select * from {{ ref('view_model') }}
