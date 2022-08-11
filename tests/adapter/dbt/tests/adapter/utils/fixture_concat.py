# concat

seeds__data_concat_csv = """input_1,input_2,output
a,b,ab
a,,a
,b,b
,,
"""


models__test_concat_sql = """
with data as (

    select * from {{ ref('data_concat') }}

)

select
    {{ concat(['input_1', 'input_2']) }} as actual,
    output as expected

from data
"""


models__test_concat_yml = """
version: 2
models:
  - name: test_concat
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
