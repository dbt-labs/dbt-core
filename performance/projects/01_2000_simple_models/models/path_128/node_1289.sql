select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_8') }}
union all
select * from {{ ref('node_17') }}
union all
select * from {{ ref('node_25') }}
union all
select * from {{ ref('node_47') }}
union all
select * from {{ ref('node_58') }}
union all
select * from {{ ref('node_332') }}
union all
select * from {{ ref('node_378') }}
union all
select * from {{ ref('node_438') }}