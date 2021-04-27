{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'type': 'non_existent_type'},
    ]
  )
}}

select 1 as column_a, 2 as column_b
