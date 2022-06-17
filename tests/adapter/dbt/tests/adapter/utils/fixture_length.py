# length

seeds__data_length_csv = """expression,output
abcdef,6
fishtown,8
december,8
www.google.com/path,19
"""


models__test_length_sql = """
with data as (

    select * from {{ ref('data_length') }}

)

select

    {{ length('expression') }} as actual,
    output as expected

from data
"""


models__test_length_yml = """
version: 2
models:
  - name: test_length
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
