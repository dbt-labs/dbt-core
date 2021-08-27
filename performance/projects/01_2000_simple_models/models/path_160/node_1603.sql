select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_93') }}
union all
select * from {{ ref('node_250') }}
union all
select * from {{ ref('node_608') }}