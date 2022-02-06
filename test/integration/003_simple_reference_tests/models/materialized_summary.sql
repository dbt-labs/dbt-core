{{
  config(
    materialized = "table"
  )
}}

select gender, count(*) as ct from {{ref('materialized_copy')}}
group by gender
order by gender asc
