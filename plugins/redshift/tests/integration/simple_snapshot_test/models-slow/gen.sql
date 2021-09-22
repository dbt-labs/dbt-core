
{{ config(materialized='ephemeral') }}


/*
    Generates 50 rows that "appear" to update every
    second to a query-er.

    1	2020-04-21 20:44:00-04	0
    2	2020-04-21 20:43:59-04	59
    3	2020-04-21 20:43:58-04	58
    4	2020-04-21 20:43:57-04	57

    .... 1 second later ....

    1	2020-04-21 20:44:01-04	1
    2	2020-04-21 20:44:00-04	0
    3	2020-04-21 20:43:59-04	59
    4	2020-04-21 20:43:58-04	58

    This view uses pg_sleep(2) to make queries against
    the view take a non-trivial amount of time

    Use statement_timestamp() as it changes during a transactions.
    If we used now() or current_time or similar, then the timestamp
    of the start of the transaction would be returned instead.
*/

with gen as (

    select
        id,
        date_trunc('second', statement_timestamp()) - (interval '1 second' * id) as updated_at

    from generate_series(1, 10) id

)

select 
    id,
    updated_at,
    extract(seconds from updated_at)::int as seconds

from gen, pg_sleep(2)
