{{
  config(
    materialized = "ephemeral"
  )
}}

select * from "simple_dependency_003"."seed"
