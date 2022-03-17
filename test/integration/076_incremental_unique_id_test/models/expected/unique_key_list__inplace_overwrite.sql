{{
    config(
        materialized='table'
    )
}}

select
    'CT'::varchar(2) as state,
    'Hartford'::varchar(12) as county,
    'Hartford'::varchar(12) as city,
    '2022-02-14'::date as last_visit_date
union all
select 'MA'::varchar(2),'Suffolk'::varchar(12),'Boston'::varchar(12),'2020-02-12'::date
union all
select 'NJ'::varchar(2),'Mercer'::varchar(12),'Trenton'::varchar(12),'2022-01-01'::date
union all
select 'NY'::varchar(2),'Kings'::varchar(12),'Brooklyn'::varchar(12),'2021-04-02'::date
union all
select 'NY'::varchar(2),'New York'::varchar(12),'Manhattan'::varchar(12),'2021-04-01'::date
union all
select 'PA'::varchar(2),'Philadelphia'::varchar(12),'Philadelphia'::varchar(12),'2021-05-21'::date
