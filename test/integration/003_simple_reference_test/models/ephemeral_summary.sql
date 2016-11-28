{{
  config(
    materialized = "table"
  )
}}

select gender, count(*) as ct from {{ref('ephemeral_copy')}}
group by gender
order by gender asc
