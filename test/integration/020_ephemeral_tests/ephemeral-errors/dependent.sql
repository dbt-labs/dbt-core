-- base copy is an error
select * from {{ref('base_copy')}} where gender = 'Male'
