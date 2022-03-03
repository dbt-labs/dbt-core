select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_5') }}
union all
select * from {{ ref('node_275') }}
union all
select * from {{ ref('node_491') }}
union all
select * from {{ ref('node_1473') }}