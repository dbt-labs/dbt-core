select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_11') }}
union all
select * from {{ ref('node_530') }}
union all
select * from {{ ref('node_628') }}
