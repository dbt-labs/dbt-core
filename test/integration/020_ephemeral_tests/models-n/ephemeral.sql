{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ref("ephemeral_level_two")}}
