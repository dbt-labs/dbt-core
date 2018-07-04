
{{
  config(
    materialized = "incremental",
    sql_where = "id > (select max(id) from {{this}})"
  )
}}

select id from {{ ref('data_seed') }}
