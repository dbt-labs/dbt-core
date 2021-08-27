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
select * from {{ ref('node_109') }}
union all
select * from {{ ref('node_370') }}
union all
select * from {{ ref('node_386') }}
union all
select * from {{ ref('node_542') }}
union all
select * from {{ ref('node_635') }}