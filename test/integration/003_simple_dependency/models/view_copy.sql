{{
  config(
    materialized = "view"
  )
}}

select * from "simple_dependency_003"."seed"
