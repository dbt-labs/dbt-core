select * from {{ref('female_only')}}
union all
select * from {{ref('double_dependent')}} where gender = 'Male'
