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
select * from {{ ref('node_68') }}
union all
select * from {{ ref('node_82') }}
union all
select * from {{ ref('node_379') }}
