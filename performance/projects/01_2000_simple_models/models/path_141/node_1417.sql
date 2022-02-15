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
select * from {{ ref('node_15') }}
union all
select * from {{ ref('node_435') }}
union all
select * from {{ ref('node_531') }}
union all
select * from {{ ref('node_648') }}
union all
select * from {{ ref('node_1036') }}
union all
select * from {{ ref('node_1143') }}
