# last_day

seeds__data_last_day_csv = """date_day,date_part,result
2018-01-02,month,2018-01-31
2018-01-02,quarter,2018-03-31
2018-01-02,year,2018-12-31
,month,
"""


models__test_last_day_sql = """
with data as (

    select * from {{ ref('data_last_day') }}

)

select
    case
        when date_part = 'month' then {{ last_day('date_day', 'month') }}
        when date_part = 'quarter' then {{ last_day('date_day', 'quarter') }}
        when date_part = 'year' then {{ last_day('date_day', 'year') }}
        else null
    end as actual,
    result as expected

from data
"""


models__test_last_day_yml = """
version: 2
models:
  - name: test_last_day
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
