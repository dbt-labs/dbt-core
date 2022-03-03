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
select * from {{ ref('node_59') }}
union all
select * from {{ ref('node_63') }}
union all
select * from {{ ref('node_71') }}
union all
select * from {{ ref('node_679') }}
union all
select * from {{ ref('node_802') }}