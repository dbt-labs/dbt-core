models__sql_header = """
{% call set_sql_header(config) %}
with _variables as (
    select 1 as my_variable
)
{%- endcall %}
select my_variable from _variables
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

models__sample_model = """
select * from {{ ref('sample_seed') }}
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
