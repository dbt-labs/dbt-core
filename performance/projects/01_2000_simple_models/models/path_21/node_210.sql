select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_24') }}
union all
select * from {{ ref('node_30') }}
union all
select * from {{ ref('node_39') }}
union all
select * from {{ ref('node_121') }}
