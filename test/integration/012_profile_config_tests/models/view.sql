{{
  config(
    materialized = "view"
  )
}}

select * from "profile_config_012"."seed"
