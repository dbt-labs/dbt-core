# right

seeds__data_right_csv = """string_text,length_expression,output
abcdef,3,def
fishtown,4,town
december,5,ember
december,0,
"""


models__test_right_sql = """
with data as (

    select * from {{ ref('data_right') }}

)

select

    {{ right('string_text', 'length_expression') }} as actual,
    coalesce(output, '') as expected

from data
"""


models__test_right_yml = """
version: 2
models:
  - name: test_right
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
