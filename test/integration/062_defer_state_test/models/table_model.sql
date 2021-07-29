{{ config(materialized='table') }}
select * from {{ ref('ephemeral_model') }}
-- {{ my_macro() }}