
{{ config(materialized='incremental', unique_key='id') }}

-- this will fail on snowflake with "merge" due
-- to the nondeterministic join on id

select 1 as id
union all
select 1 as id
