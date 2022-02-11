select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_7') }}
union all
select * from {{ ref('node_139') }}
union all
select * from {{ ref('node_1016') }}
union all
select * from {{ ref('node_1052') }}
union all
select * from {{ ref('node_1172') }}
