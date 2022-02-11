select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_18') }}
union all
select * from {{ ref('node_149') }}
union all
select * from {{ ref('node_293') }}
union all
select * from {{ ref('node_294') }}
