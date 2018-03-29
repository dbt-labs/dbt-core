{{
  config(
    materialized = "view",
    enabled = False
  )
}}

select * from {{ target.schema }}.seed
