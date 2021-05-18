{{
  config(
    materialized = "table",
    indexes=[
      {'columns': 'column_a, column_b'},
    ]
  )
}}

select 1 as column_a, 2 as column_b
