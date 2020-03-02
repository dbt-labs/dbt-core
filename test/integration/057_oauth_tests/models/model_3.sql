select * from {{ ref('model_1') }}
union all
select * from {{ ref('model_2') }}
