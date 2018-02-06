{{
  config(
    materialized = "table",
    sort = 'first_name',
    sort_type = 'compound'
  )
}}

select * from {{ target.schema }}.seed
