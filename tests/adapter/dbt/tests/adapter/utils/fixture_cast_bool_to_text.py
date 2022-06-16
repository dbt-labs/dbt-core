# cast_bool_to_text

models__test_cast_bool_to_text_sql = """
with data as (

    select 0=1 as input, 'false' as expected union all
    select 1=1 as input, 'true' as expected union all
    select null as input, null as expected

)

select

    {{ cast_bool_to_text("input") }} as actual,
    expected

from data
"""


models__test_cast_bool_to_text_yml = """
version: 2
models:
  - name: test_cast_bool_to_text
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
