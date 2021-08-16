select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_4') }}
union all
select * from {{ ref('node_64') }}
union all
select * from {{ ref('node_228') }}
union all
select * from {{ ref('node_255') }}
union all
select * from {{ ref('node_591') }}
union all
select * from {{ ref('node_1568') }}