{{
  config(
    materialized = "table"
  )
}}

-- this is a unicode character: å
select * from {{ target.schema }}.seed
