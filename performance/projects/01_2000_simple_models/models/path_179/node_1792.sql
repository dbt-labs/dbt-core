select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_5') }}
union all
select * from {{ ref('node_57') }}
union all
select * from {{ ref('node_246') }}
union all
select * from {{ ref('node_456') }}
union all
select * from {{ ref('node_743') }}