
select * from {{ ref('model_a') }}
union all
select * from {{ ref('model_e') }}
