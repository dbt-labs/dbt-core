{{
  config(
    materialized = "view"
  )
}}

select * from {{ this.schema }}.seed
