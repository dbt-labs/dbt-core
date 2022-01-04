select * from {{ref('table_a')}}

union all

select * from {{ref('table_b')}}
