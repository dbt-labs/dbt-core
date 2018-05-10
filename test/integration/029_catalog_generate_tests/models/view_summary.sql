
{{
    config(
        materialized='view'
    )
}}


with t as (

    select * from {{ ref('view_model') }}

)

select date_trunc('year', updated_at) as year,
       count(*)
from t
group by 1
