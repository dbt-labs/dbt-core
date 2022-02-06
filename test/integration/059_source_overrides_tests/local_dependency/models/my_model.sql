{{ config(materialized='table') }}
with colors as (
	select id, color from {{ source('my_source', 'my_table') }}
),
letters as (
	select id, letter from {{ source('my_source', 'my_other_table') }}
)
select letter, color from colors join letters using (id)
