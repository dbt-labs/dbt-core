# escape_single_quotes

models__test_escape_single_quotes_quote_sql = """
select
  '{{ escape_single_quotes("they're") }}' as actual,
  'they''re' as expected,
  {{ length(string_literal(escape_single_quotes("they're"))) }} as actual_length,
  7 as expected_length

union all

select
  '{{ escape_single_quotes("they are") }}' as actual,
  'they are' as expected,
  {{ length(string_literal(escape_single_quotes("they are"))) }} as actual_length,
  8 as expected_length
"""


# The expected literal is 'they\'re'. The second backslash is to escape it from Python.
models__test_escape_single_quotes_backslash_sql = """
select
  '{{ escape_single_quotes("they're") }}' as actual,
  'they\\'re' as expected,
  {{ length(string_literal(escape_single_quotes("they're"))) }} as actual_length,
  7 as expected_length

union all

select
  '{{ escape_single_quotes("they are") }}' as actual,
  'they are' as expected,
  {{ length(string_literal(escape_single_quotes("they are"))) }} as actual_length,
  8 as expected_length
"""


models__test_escape_single_quotes_yml = """
version: 2
models:
  - name: test_escape_single_quotes
    tests:
      - assert_equal:
          actual: actual
          expected: expected
      - assert_equal:
          actual: actual_length
          expected: expected_length
"""
