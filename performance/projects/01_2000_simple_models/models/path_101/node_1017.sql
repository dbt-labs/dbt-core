select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_13') }}
union all
select * from {{ ref('node_38') }}
union all
select * from {{ ref('node_91') }}
union all
select * from {{ ref('node_220') }}
union all
select * from {{ ref('node_402') }}
union all
select * from {{ ref('node_620') }}