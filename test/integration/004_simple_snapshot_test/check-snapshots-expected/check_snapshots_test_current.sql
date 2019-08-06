

with query as (

    -- check that the current value for id=1 is red
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 1 and color = 'red' and dbt_valid_to is null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that the previous 'red' value for id=1 is invalidated
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 1 and color = 'red' and dbt_valid_to is not null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that there's only one current record for id=2
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 2 and color = 'pink' and dbt_valid_to is null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that the previous value for id=2 is represented
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 2 and color = 'green' and dbt_valid_to is not null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that there are 5 records total in the table
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
    ) = 5 then 0 else 1 end as failures

)

select *
from query
where failures = 1
