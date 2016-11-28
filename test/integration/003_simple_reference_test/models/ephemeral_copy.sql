{{
  config(
    materialized = "ephemeral"
  )
}}

select * from "simple_reference_003"."seed"
