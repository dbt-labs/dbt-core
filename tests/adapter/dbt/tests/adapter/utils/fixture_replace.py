# replace

seeds__data_replace_csv = """string_text,search_chars,replace_chars,result
a,a,b,b
http://google.com,http://,"",google.com
"""


models__test_replace_sql = """
with data as (

    select

        *,
        coalesce(search_chars, '') as old_chars,
        coalesce(replace_chars, '') as new_chars

    from {{ ref('data_replace') }}

)

select

    {{ replace('string_text', 'old_chars', 'new_chars') }} as actual,
    result as expected

from data
"""


models__test_replace_yml = """
version: 2
models:
  - name: test_replace
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
