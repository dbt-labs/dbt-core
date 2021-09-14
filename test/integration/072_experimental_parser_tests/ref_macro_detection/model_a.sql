{{ config(tags='hello', x=False) }}
{{ config(tags='world', x=True) }}

select * from {{ ref('model_b') }}
cross join {{ source('my_src', 'my_tbl') }}
where false
