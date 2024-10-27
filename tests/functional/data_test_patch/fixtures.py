tests__my_singular_test_sql = """
with my_cte as (
    select 1 as id, 'foo' as name
    union all
    select 2 as id, 'bar' as name
)
select * from my_cte
"""

tests__schema_yml = """
data_tests:
  - name: my_singular_test
    description: "{{ doc('my_singular_test_documentation') }}"
    config:
      error_if: ">10"
    meta:
      some_key: some_val
"""

tests__doc_block_md = """
{% docs my_singular_test_documentation %}

Some docs from a doc block

{% enddocs %}
"""

tests__invalid_name_schema_yml = """
data_tests:
  - name: my_double_test
    description: documentation, but make it double
"""

tests__malformed_schema_yml = """
data_tests: &not_null
  - not_null:
      where: some_condition
"""
