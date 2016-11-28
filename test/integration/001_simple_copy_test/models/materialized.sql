{{
  config(
    materialized = "table"
  )
}}

select * from "simple_copy_001"."seed"
