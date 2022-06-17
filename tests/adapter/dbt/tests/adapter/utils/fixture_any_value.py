# any_value

seeds__data_any_value_csv = """id,key_name,static_col
1,abc,dbt
2,abc,dbt
3,jkl,dbt
4,jkl,dbt
5,jkl,dbt
6,xyz,test
"""


seeds__data_any_value_expected_csv = """key_name,static_col,num_rows
abc,dbt,2
jkl,dbt,3
xyz,test,1
"""


models__test_any_value_sql = """
with data as (

    select * from {{ ref('data_any_value') }}

),

data_output as (

    select * from {{ ref('data_any_value_expected') }}

),

calculate as (
    select
        key_name,
        {{ any_value('static_col') }} as static_col,
        count(id) as num_rows
    from data
    group by key_name
)

select
    calculate.num_rows as actual,
    data_output.num_rows as expected
from calculate
left join data_output
on calculate.key_name = data_output.key_name
and calculate.static_col = data_output.static_col
"""


models__test_any_value_yml = """
version: 2
models:
  - name: test_any_value
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
