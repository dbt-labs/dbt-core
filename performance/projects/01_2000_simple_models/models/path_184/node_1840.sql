select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_24') }}
union all
select * from {{ ref('node_30') }}
union all
select * from {{ ref('node_348') }}
union all
select * from {{ ref('node_1049') }}
