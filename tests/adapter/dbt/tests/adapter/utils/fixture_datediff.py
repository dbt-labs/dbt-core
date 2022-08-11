# datediff

seeds__data_datediff_csv = """first_date,second_date,datepart,result
2018-01-01 01:00:00,2018-01-02 01:00:00,day,1
2018-01-01 01:00:00,2018-02-01 01:00:00,month,1
2018-01-01 01:00:00,2019-01-01 01:00:00,year,1
2018-01-01 01:00:00,2018-01-01 02:00:00,hour,1
2018-01-01 01:00:00,2018-01-01 02:01:00,minute,61
2018-01-01 01:00:00,2018-01-01 02:00:01,second,3601
2019-12-31 00:00:00,2019-12-27 00:00:00,week,-1
2019-12-31 00:00:00,2019-12-30 00:00:00,week,0
2019-12-31 00:00:00,2020-01-02 00:00:00,week,0
2019-12-31 00:00:00,2020-01-06 02:00:00,week,1
,2018-01-01 02:00:00,hour,
2018-01-01 02:00:00,,hour,
"""


models__test_datediff_sql = """
with data as (

    select * from {{ ref('data_datediff') }}

)

select

    case
        when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
        when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
        when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
        when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
        when datepart = 'week' then {{ datediff('first_date', 'second_date', 'week') }}
        when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
        when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
        else null
    end as actual,
    result as expected

from data

-- Also test correct casting of literal values.

union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "microsecond") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "millisecond") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "second") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "minute") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "hour") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "day") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-03 00:00:00.000000'", "week") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "month") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "quarter") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "year") }} as actual, 1 as expected
"""


models__test_datediff_yml = """
version: 2
models:
  - name: test_datediff
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
