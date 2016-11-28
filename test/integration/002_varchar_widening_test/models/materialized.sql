{{
  config(
    materialized = "table"
  )
}}

select * from "varchar_widening_002"."seed"
