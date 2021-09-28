{{
  config(
    materialized = "table",
    sort = ['first_name', 'last_name'],
    sort_type = 'interleaved'
  )
}}

select * from {{ ref('seed') }}
