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
select * from {{ ref('node_10') }}
union all
select * from {{ ref('node_48') }}
union all
select * from {{ ref('node_67') }}
union all
select * from {{ ref('node_141') }}
union all
select * from {{ ref('node_212') }}
union all
select * from {{ ref('node_262') }}
union all
select * from {{ ref('node_450') }}
