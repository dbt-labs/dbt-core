select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_24') }}
union all
select * from {{ ref('node_140') }}
union all
select * from {{ ref('node_320') }}
union all
select * from {{ ref('node_797') }}
union all
select * from {{ ref('node_1392') }}