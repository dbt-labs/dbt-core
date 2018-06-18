{{
  config(
    materialized = "view",
    enabled = False
  )
}}

select * from {{ ref('seed') }}
