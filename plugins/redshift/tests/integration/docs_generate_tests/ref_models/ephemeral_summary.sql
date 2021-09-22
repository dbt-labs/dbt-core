{{
  config(
    materialized = "table"
  )
}}

select first_name, count(*) as ct from {{ref('ephemeral_copy')}}
group by first_name
order by first_name asc
