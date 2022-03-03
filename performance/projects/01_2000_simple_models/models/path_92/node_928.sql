select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_205') }}
union all
select * from {{ ref('node_399') }}
union all
select * from {{ ref('node_554') }}
union all
select * from {{ ref('node_645') }}