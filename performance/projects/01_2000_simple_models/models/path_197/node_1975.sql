select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_73') }}
union all
select * from {{ ref('node_138') }}
union all
select * from {{ ref('node_238') }}
union all
select * from {{ ref('node_295') }}
union all
select * from {{ ref('node_495') }}
union all
select * from {{ ref('node_533') }}
union all
select * from {{ ref('node_703') }}
union all
select * from {{ ref('node_874') }}
union all
select * from {{ ref('node_1113') }}
union all
select * from {{ ref('node_1333') }}
