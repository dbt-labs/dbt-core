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
select * from {{ ref('node_78') }}
union all
select * from {{ ref('node_97') }}
union all
select * from {{ ref('node_129') }}
union all
select * from {{ ref('node_130') }}
union all
select * from {{ ref('node_442') }}
union all
select * from {{ ref('node_1131') }}