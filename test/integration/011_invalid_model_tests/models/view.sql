{{
  config(
    materialized = "view",
    sql_where    = "id > (select max(id) from {{ this }})",
    sort = "id",
    dist = "id"
  )
}}

select * from "invalid_models_011"."seed"
