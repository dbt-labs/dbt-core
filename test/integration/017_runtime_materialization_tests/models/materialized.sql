{{
  config(
    materialized = "table"
  )
}}

select * from "runtime_materialization_017"."seed"
