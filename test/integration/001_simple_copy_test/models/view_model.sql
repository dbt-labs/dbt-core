{{
  config(
    materialized = "view"
  )
}}

select * from {{ ref('seed') }}
