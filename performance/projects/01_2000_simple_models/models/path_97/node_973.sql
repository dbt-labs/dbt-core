select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_4') }}
union all
select * from {{ ref('node_87') }}
union all
select * from {{ ref('node_186') }}
union all
select * from {{ ref('node_203') }}
union all
select * from {{ ref('node_221') }}
