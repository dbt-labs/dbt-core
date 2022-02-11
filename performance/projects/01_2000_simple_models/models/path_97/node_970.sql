select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_13') }}
union all
select * from {{ ref('node_66') }}
union all
select * from {{ ref('node_79') }}
union all
select * from {{ ref('node_184') }}
union all
select * from {{ ref('node_233') }}
union all
select * from {{ ref('node_410') }}
union all
select * from {{ ref('node_424') }}
union all
select * from {{ ref('node_467') }}
