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
select * from {{ ref('node_178') }}
union all
select * from {{ ref('node_653') }}
union all
select * from {{ ref('node_672') }}
union all
select * from {{ ref('node_873') }}
union all
select * from {{ ref('node_1014') }}
union all
select * from {{ ref('node_1059') }}
union all
select * from {{ ref('node_1372') }}