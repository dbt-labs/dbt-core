{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_b']},
      {'columns': ['column_a', 'column_b']},
      {'columns': ['column_b', 'column_a'], 'type': 'btree', 'unique': True},
      {'columns': ['column_a'], 'type': 'hash'}
    ]
  )
}}

select 1 as column_a, 2 as column_b
