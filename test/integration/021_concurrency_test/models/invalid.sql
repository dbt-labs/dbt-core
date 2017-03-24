{{
  config(
    materialized = "table"
  )
}}

select a_field_that_does_not_exist from "concurrency_021"."seed"
