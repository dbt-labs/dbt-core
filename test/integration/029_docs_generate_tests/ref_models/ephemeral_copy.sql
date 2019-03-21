{{
  config(
    materialized = "ephemeral"
  )
}}

select * from {{ source("my_source", "my_table") }}
