MODELS__TEST_MIXED_NULL_COMPARE_SQL = """
select
    1 as actual,
    null as expected
"""


MODELS__TEST_MIXED_NULL_COMPARE_YML = """
version: 2
models:
  - name: test_mixed_null_compare
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""


MODELS__TEST_NULL_COMPARE_SQL = """
select
    null as actual,
    null as expected
"""


MODELS__TEST_NULL_COMPARE_YML = """
version: 2
models:
  - name: test_null_compare
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
