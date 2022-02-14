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
select * from {{ ref('node_72') }}
union all
select * from {{ ref('node_75') }}
union all
select * from {{ ref('node_231') }}
union all
select * from {{ ref('node_385') }}
union all
select * from {{ ref('node_561') }}
