
select * from {{ ref('example') }}
union all
select * from {{ ref('example') }}
