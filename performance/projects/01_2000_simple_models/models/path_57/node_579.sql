select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_312') }}
union all
select * from {{ ref('node_333') }}
union all
select * from {{ ref('node_426') }}
union all
select * from {{ ref('node_515') }}