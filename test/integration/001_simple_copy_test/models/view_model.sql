{{
  config(
    materialized = "view"
  )
}}

select * from {{ target.schema }}.seed
