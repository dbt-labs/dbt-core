{{ config(materialized='table') }}

select * from {{ ref('countries') }}