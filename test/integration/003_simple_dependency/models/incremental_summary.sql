{{
  config(
    materialized = "incremental",
    sql_where = "id>(select max(id) from {{this}})"
  )
}}

select gender, count(*) as ct from {{ref('incremental_copy')}}
group by gender
order by gender asc
