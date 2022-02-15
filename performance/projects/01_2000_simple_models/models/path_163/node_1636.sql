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
select * from {{ ref('node_36') }}
union all
select * from {{ ref('node_285') }}
union all
select * from {{ ref('node_337') }}
union all
select * from {{ ref('node_818') }}
union all
select * from {{ ref('node_1024') }}
