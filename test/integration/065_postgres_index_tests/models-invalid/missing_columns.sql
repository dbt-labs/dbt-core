{{
  config(
    materialized = "table",
    indexes=[
      {'unique': True},
    ]
  )
}}

select 1 as column_a, 2 as column_b
