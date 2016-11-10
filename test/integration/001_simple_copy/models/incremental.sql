{{
  config(
    materialized = "incremental",
    sql_where = "id>(select max(id) from {{this}})"
  )
}}

select * from "simple_copy_001"."seed"
