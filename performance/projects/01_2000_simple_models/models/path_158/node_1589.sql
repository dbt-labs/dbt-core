select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_13') }}
union all
select * from {{ ref('node_28') }}
union all
select * from {{ ref('node_315') }}
union all
select * from {{ ref('node_532') }}
union all
select * from {{ ref('node_813') }}
union all
select * from {{ ref('node_996') }}