{{
  config(
    materialized = "incremental",
    sql_where = "id>(select max(id) from {{this}})"
  )
}}

select * from "varchar_widening_002"."seed"
