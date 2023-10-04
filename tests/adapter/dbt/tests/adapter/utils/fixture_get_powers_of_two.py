# get_powers_of_two

models__test_get_powers_of_two_sql = """
select {{ get_powers_of_two(1) }} as actual, 1 as expected

union all

select {{ get_powers_of_two(4) }} as actual, 2 as expected

union all

select {{ get_powers_of_two(27) }} as actual, 5 as expected

union all

select {{ get_powers_of_two(256) }} as actual, 8 as expected

union all

select {{ get_powers_of_two(3125) }} as actual, 12 as expected

union all

select {{ get_powers_of_two(46656) }} as actual, 16 as expected

union all

select {{ get_powers_of_two(823543) }} as actual, 20 as expected
"""

models__test_get_powers_of_two_yml = """
version: 2
models:
  - name: test_powers_of_two
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
