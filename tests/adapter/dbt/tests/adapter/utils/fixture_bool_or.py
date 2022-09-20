# bool_or

seeds__data_bool_or_csv = """key_column,val1,val2
abc,1,1
abc,1,0
def,1,0
hij,1,1
hij,1,
klm,1,0
klm,1,
"""


seeds__data_bool_or_expected_csv = """key_column,value
abc,true
def,false
hij,true
klm,false
"""


models__test_bool_or_sql = """
with data as (

    select * from {{ ref('data_bool_or') }}

),

data_output as (

    select * from {{ ref('data_bool_or_expected') }}

),

calculate as (

    select
        key_column,
        {{ bool_or('val1 = val2') }} as value
    from data
    group by key_column

)

select
    calculate.value as actual,
    data_output.value as expected
from calculate
left join data_output
on calculate.key_column = data_output.key_column
"""


models__test_bool_or_yml = """
version: 2
models:
  - name: test_bool_or
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
