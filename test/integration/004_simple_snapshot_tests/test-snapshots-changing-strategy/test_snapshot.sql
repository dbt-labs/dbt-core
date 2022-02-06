
{# /*
    Given the repro case for the snapshot build, we'd
    expect to see both records have color='pink'
    in their most recent rows.
*/ #}

with expected as (

    select 1 as id, 'pink' as color union all
    select 2 as id, 'pink' as color

),

actual as (

    select id, color
    from {{ ref('my_snapshot') }}
    where color = 'pink'
      and dbt_valid_to is null

)

select * from expected
except
select * from actual

union all

select * from actual
except
select * from expected
