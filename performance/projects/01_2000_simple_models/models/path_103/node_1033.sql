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
select * from {{ ref('node_20') }}
union all
select * from {{ ref('node_98') }}
union all
select * from {{ ref('node_199') }}
union all
select * from {{ ref('node_236') }}