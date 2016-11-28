{{
  config(
    materialized = "view"
  )
}}

select gender, count(*) as ct from {{ref('view_copy')}}
group by gender
order by gender asc
