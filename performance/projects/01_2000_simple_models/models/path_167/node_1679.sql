select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_311') }}
union all
select * from {{ ref('node_423') }}
union all
select * from {{ ref('node_586') }}
union all
select * from {{ ref('node_985') }}
