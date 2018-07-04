
{{
  config(
    materialized = "incremental",
    sql_where = "true",
    unique_key = "id"
  )
}}

select id from {{ ref('data_seed') }}
