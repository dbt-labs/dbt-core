select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_73') }}
union all
select * from {{ ref('node_138') }}
union all
select * from {{ ref('node_238') }}
union all
select * from {{ ref('node_295') }}
union all
select * from {{ ref('node_1187') }}