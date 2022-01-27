{{
  config(
    materialized = "table"
  )
}}

select * from {{ref('view_model')}}
