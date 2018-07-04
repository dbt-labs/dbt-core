{{
  config(
    materialized = "table",
  )
}}

select * from {{ ref('data_seed') }}