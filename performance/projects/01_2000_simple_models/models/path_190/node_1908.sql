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
select * from {{ ref('node_308') }}
union all
select * from {{ ref('node_474') }}
union all
select * from {{ ref('node_1193') }}
union all
select * from {{ ref('node_1464') }}