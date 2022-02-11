select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_7') }}
union all
select * from {{ ref('node_52') }}
union all
select * from {{ ref('node_101') }}
union all
select * from {{ ref('node_211') }}
union all
select * from {{ ref('node_243') }}
