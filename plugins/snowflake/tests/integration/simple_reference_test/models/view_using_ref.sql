{{
  config(
    materialized = "view"
  )
}}

select gender, count(*) as ct from {{ var('var_ref') }}
group by gender
order by gender asc
