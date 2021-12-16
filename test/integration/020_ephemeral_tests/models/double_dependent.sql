
-- base_copy just pulls from base. Make sure the listed
-- graph of CTEs all share the same dbt_cte__base cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base_copy')}} where gender = 'Female'
