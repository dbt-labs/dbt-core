{{
    config(
        materialized='table'
    )
}}

-- force a foreign key constraint failure here
select 105 as id, count(*) as count
from {{ ref('table_failure_copy') }}
group by 1
