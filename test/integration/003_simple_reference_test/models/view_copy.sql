{{
  config(
    materialized = "view"
  )
}}

select * from "simple_reference_003"."seed"
