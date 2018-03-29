{{
  config(
    materialized = "view"
  )
}}

select 1 as id, current_date as updated_at, 'a' as dupe
union all
select 2 as id, current_date as updated_at, 'a' as dupe
union all
select 3 as id, current_date as updated_at, 'a' as dupe
union all
select 4 as id, current_date as updated_at, 'a' as dupe
