{{
  config(
    materialized = "table"
  )
}}

-- this is a unicode character: å
select * from "simple_copy_001"."seed"
