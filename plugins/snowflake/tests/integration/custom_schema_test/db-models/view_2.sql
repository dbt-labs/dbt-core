{{ config(database='alt') }}
select * from {{ ref('view_1') }}
