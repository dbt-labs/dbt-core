select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_16') }}
union all
select * from {{ ref('node_61') }}
union all
select * from {{ ref('node_406') }}