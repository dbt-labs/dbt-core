select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_11') }}
union all
select * from {{ ref('node_14') }}
union all
select * from {{ ref('node_41') }}
union all
select * from {{ ref('node_120') }}
union all
select * from {{ ref('node_946') }}
