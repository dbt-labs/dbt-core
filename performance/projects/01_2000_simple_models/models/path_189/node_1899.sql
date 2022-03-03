select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_13') }}
union all
select * from {{ ref('node_45') }}
union all
select * from {{ ref('node_62') }}
union all
select * from {{ ref('node_158') }}
union all
select * from {{ ref('node_222') }}
union all
select * from {{ ref('node_655') }}
union all
select * from {{ ref('node_823') }}
union all
select * from {{ ref('node_1140') }}