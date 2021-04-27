select * from {{ ref('model_a') }}
cross join {{ source('my_src', 'my_tbl') }}
where false
