select * from {{ ref('model_a') }}
cross join {{ ref('model_b') }}
where false
