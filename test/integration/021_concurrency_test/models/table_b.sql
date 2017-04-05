{{
  config(
    materialized = "table"
  )
}}

select * from "concurrency_021"."seed"
