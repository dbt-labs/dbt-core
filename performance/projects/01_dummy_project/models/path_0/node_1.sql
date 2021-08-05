select 1 as id
union all
select * from {{ ref('node_0') }}