
{{
    config(
        materialized='table'
    )
}}


select date_trunc('year', updated_at) as year,
       count(*)
from dry_run_007.seed
group by 1
