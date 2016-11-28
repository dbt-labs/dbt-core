{{
  config(
    materialized = "incremental",
    sql_where = "id>(select max(id) from {{this}})"
  )
}}

select * from "simple_reference_003"."seed"
