# date_trunc

seeds__data_date_trunc_csv = """updated_at,day,month
2018-01-05 12:00:00,2018-01-05,2018-01-01
,,
"""


models__test_date_trunc_sql = """
with data as (

    select * from {{ ref('data_date_trunc') }}

)

select
    cast({{date_trunc('day', 'updated_at') }} as date) as actual,
    day as expected

from data

union all

select
    cast({{ date_trunc('month', 'updated_at') }} as date) as actual,
    month as expected

from data
"""


models__test_date_trunc_yml = """
version: 2
models:
  - name: test_date_trunc
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
