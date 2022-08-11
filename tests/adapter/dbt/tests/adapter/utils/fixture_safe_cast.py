# safe_cast

seeds__data_safe_cast_csv = """field,output
abc,abc
123,123
,
"""


models__test_safe_cast_sql = """
with data as (

    select * from {{ ref('data_safe_cast') }}

)

select
    {{ safe_cast('field', api.Column.translate_type('string')) }} as actual,
    output as expected

from data
"""


models__test_safe_cast_yml = """
version: 2
models:
  - name: test_safe_cast
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
