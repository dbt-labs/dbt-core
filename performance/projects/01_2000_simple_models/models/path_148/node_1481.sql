select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_180') }}
union all
select * from {{ ref('node_407') }}
union all
select * from {{ ref('node_480') }}
union all
select * from {{ ref('node_1343') }}