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
select * from {{ ref('node_380') }}
union all
select * from {{ ref('node_851') }}
union all
select * from {{ ref('node_1021') }}