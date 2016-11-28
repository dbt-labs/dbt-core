{{
  config(
    materialized = "table"
  )
}}

select * from "simple_reference_003"."seed"
