MODEL_BASE_TABLE = """
{{ config(
    materialized='table',
    backup=False
) }}
select 1 as my_col
"""


MODEL_MAT_VIEW = """
{{ config(
    materialized='materialized_view'
) }}
select * from {{ ref('base_table') }}
"""
