# equals

SEEDS__DATA_EQUALS_CSV = """key_name,x,y,expected
1,1,1,same
2,1,2,different
3,1,null,different
4,2,1,different
5,2,2,same
6,2,null,different
7,null,1,different
8,null,2,different
9,null,null,same
"""


MODELS__EQUAL_VALUES_SQL = """
with data as (

    select * from {{ ref('data_equals') }}

)

select *
from data
where
  {{ equals('x', 'y') }}
"""


MODELS__NOT_EQUAL_VALUES_SQL = """
with data as (

    select * from {{ ref('data_equals') }}

)

select *
from data
where
  not {{ equals('x', 'y') }}
"""
