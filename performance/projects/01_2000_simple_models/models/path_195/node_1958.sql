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
select * from {{ ref('node_43') }}
union all
select * from {{ ref('node_230') }}
union all
select * from {{ ref('node_481') }}