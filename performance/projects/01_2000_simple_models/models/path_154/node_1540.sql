select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_77') }}
union all
select * from {{ ref('node_134') }}
union all
select * from {{ ref('node_397') }}
union all
select * from {{ ref('node_1495') }}