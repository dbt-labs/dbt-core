{{ config(materialized='table') }}
select * from {{ ref('ephemeral_model') }}

-- establish a macro dependency to trigger state:modified.macros
-- depends on: {{ my_macro() }}