select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_24') }}
union all
select * from {{ ref('node_132') }}
union all
select * from {{ ref('node_280') }}
union all
select * from {{ ref('node_623') }}
