select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_131') }}
union all
select * from {{ ref('node_226') }}
union all
select * from {{ ref('node_625') }}