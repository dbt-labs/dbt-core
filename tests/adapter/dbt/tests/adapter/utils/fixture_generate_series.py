# If generate_series works properly, there should be no `null` values in the resulting model

models__test_generate_series_sql = """
with generated_numbers as (
    {{ dbt.generate_series(10) }}
), expected_numbers as (
    select 1 as expected
    union all
    select 2 as expected
    union all
    select 3 as expected
    union all
    select 4 as expected
    union all
    select 5 as expected
    union all
    select 6 as expected
    union all
    select 7 as expected
    union all
    select 8 as expected
    union all
    select 9 as expected
    union all
    select 10 as expected
), joined as (
    select
        generated_numbers.generated_number,
        expected_numbers.expected
    from generated_numbers
    left join expected_numbers on generated_numbers.generated_number = expected_numbers.expected
)

SELECT * from joined
"""

models__test_generate_series_yml = """
version: 2
models:
  - name: test_generate_series
    tests:
      - assert_equal:
          actual: generated_number
          expected: expected
"""
