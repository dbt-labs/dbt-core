{{
  config(
    materialized = "table"
  )
}}

select * from "simple_dependency_003"."seed"
