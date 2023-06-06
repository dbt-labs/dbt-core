# concat

# https://github.com/dbt-labs/dbt-core/issues/4725
seeds__data_concat_csv = """input_1,input_2,output
a,b,ab
a,EMPTY,a
EMPTY,b,b
EMPTY,EMPTY,EMPTY
"""


models__test_concat_sql = """
with seed_data as (

    select * from {{ ref('data_concat') }}

),

data as (

    select
        {{ replace_empty('input_1') }} as input_1,
        {{ replace_empty('input_2') }} as input_2,
        {{ replace_empty('output') }} as output
    from seed_data

)

select
    {{ concat(['input_1', 'input_2']) }} as actual,
    output as expected

from data
"""


models__test_concat_yml = """
version: 2
models:
  - name: test_concat
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
