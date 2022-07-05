# hash

seeds__data_hash_csv = """input_1,output
ab,187ef4436122d1cc2f40dc2b92f0eba0
a,0cc175b9c0f1b6a831c399e269772661
1,c4ca4238a0b923820dcc509a6f75849b
,d41d8cd98f00b204e9800998ecf8427e
"""


models__test_hash_sql = """
with data as (

    select * from {{ ref('data_hash') }}

)

select
    {{ hash('input_1') }} as actual,
    output as expected

from data
"""


models__test_hash_yml = """
version: 2
models:
  - name: test_hash
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
