# string_literal

models__test_string_literal_sql = """
select {{ string_literal("abc") }} as actual, 'abc' as expected union all
select {{ string_literal("1") }} as actual, '1' as expected union all
select {{ string_literal("") }} as actual, '' as expected union all
select {{ string_literal(none) }} as actual, 'None' as expected
"""


models__test_string_literal_yml = """
version: 2
models:
  - name: test_string_literal
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
