models__sample_model = """
select * from {{ ref('sample_seed') }}
"""

models__second_model = """
select
    sample_num as col_one,
    sample_bool as col_two,
    42 as answer
from {{ ref('sample_model') }}
"""

models__ephemeral_model = """
{{ config(materialized = 'ephemeral') }}
select
    coalesce(sample_num, 0) + 10 as col_deci
from {{ ref('sample_model') }}
"""

models__second_ephemeral_model = """
{{ config(materialized = 'ephemeral') }}
select
    col_deci + 100 as col_hundo
from {{ ref('ephemeral_model') }}
"""

seeds__sample_seed = """sample_num,sample_bool
1,true
2,false
3,true
4,false
5,true
6,false
7,true
"""
