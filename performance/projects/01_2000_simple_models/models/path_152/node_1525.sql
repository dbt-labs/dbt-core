select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_4') }}
union all
select * from {{ ref('node_64') }}
union all
select * from {{ ref('node_228') }}
union all
select * from {{ ref('node_255') }}
union all
select * from {{ ref('node_511') }}
union all
select * from {{ ref('node_824') }}
union all
select * from {{ ref('node_891') }}
