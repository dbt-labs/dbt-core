MY_SEED = """
id,value,record_valid_date
1,100,2023-01-01 00:00:00
2,200,2023-01-02 00:00:00
3,300,2023-01-02 00:00:00
""".strip()


MY_TABLE = """
{{ config(
    materialized='table',
) }}
select *
from {{ ref('my_seed') }}
"""


MY_VIEW = """
{{ config(
    materialized='view',
) }}
select *
from {{ ref('my_seed') }}
"""


MY_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
) }}
select *
from {{ ref('my_seed') }}
"""
