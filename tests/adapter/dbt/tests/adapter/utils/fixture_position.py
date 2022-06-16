# position

seeds__data_position_csv = """substring_text,string_text,result
def,abcdef,4
land,earth,0
town,fishtown,5
ember,december,4
"""


models__test_position_sql = """
with data as (

    select * from {{ ref('data_position') }}

)

select

    {{ position('substring_text', 'string_text') }} as actual,
    result as expected

from data
"""


models__test_position_yml = """
version: 2
models:
  - name: test_position
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
