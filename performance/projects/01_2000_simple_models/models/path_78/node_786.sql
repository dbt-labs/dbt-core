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
select * from {{ ref('node_21') }}
union all
select * from {{ ref('node_53') }}
union all
select * from {{ ref('node_54') }}
union all
select * from {{ ref('node_195') }}
union all
select * from {{ ref('node_503') }}
union all
select * from {{ ref('node_544') }}