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
select * from {{ ref('node_69') }}
union all
select * from {{ ref('node_365') }}
union all
select * from {{ ref('node_409') }}
union all
select * from {{ ref('node_573') }}
union all
select * from {{ ref('node_876') }}
