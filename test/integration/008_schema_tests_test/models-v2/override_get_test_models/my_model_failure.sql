select * from {{ ref('my_model_pass') }}
UNION ALL
select null as id