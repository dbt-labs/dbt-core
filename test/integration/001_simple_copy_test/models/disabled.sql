{{
  config(
    materialized = "view",
    enabled = False
  )
}}

select * from "simple_copy_001"."seed"
