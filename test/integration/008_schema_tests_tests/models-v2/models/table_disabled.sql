{{
    config(
        enabled=False
    )
}}

-- force a foreign key constraint failure here
select 'purple' as favorite_color, count(*) as count
from {{ ref('table_failure_copy') }}
group by 1
