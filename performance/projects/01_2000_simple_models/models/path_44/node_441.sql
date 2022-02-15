select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_13') }}
union all
select * from {{ ref('node_45') }}
union all
select * from {{ ref('node_316') }}
union all
select * from {{ ref('node_347') }}
