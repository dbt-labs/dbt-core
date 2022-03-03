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
select * from {{ ref('node_10') }}
union all
select * from {{ ref('node_12') }}
union all
select * from {{ ref('node_105') }}
union all
select * from {{ ref('node_434') }}
union all
select * from {{ ref('node_518') }}
union all
select * from {{ ref('node_1302') }}