# split_part

seeds__data_split_part_csv = """parts,split_on,result_1,result_2,result_3
a|b|c,|,a,b,c
1|2|3,|,1,2,3
,|,,,
"""


models__test_split_part_sql = """
with data as (

    select * from {{ ref('data_split_part') }}

)

select
    {{ split_part('parts', 'split_on', 1) }} as actual,
    result_1 as expected

from data

union all

select
    {{ split_part('parts', 'split_on', 2) }} as actual,
    result_2 as expected

from data

union all

select
    {{ split_part('parts', 'split_on', 3) }} as actual,
    result_3 as expected

from data
"""


models__test_split_part_yml = """
version: 2
models:
  - name: test_split_part
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
