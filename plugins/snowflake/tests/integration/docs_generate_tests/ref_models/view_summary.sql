{{
  config(
    materialized = "view"
  )
}}

select first_name, ct from {{ref('ephemeral_summary')}}
order by ct asc
