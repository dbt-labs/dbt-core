
-- multiple ephemeral refs should share a cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base')}} where gender = 'Female'
