{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ ref('source_table') }}
