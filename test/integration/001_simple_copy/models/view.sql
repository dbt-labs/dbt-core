{{
  config(
    materialized = "view"
  )
}}

select * from "simple_copy_001"."seed"
