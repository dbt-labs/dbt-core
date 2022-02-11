select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_18') }}
union all
select * from {{ ref('node_34') }}
union all
select * from {{ ref('node_114') }}
union all
select * from {{ ref('node_404') }}
