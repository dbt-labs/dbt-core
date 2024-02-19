models__already_exists_sql = """
select 1 as id

{% if adapter.already_exists(this.schema, this.identifier) and not should_full_refresh() %}
    where id > (select max(id) from {{this}})
{% endif %}
"""

models_trivial__model_sql = """
select 1 as id
"""

macros__custom_test_sql = """
{% test custom(model) %}
  select * from {{ model }}
  limit 0
{% endtest %}
"""


bad_name_yaml = """
version: 2

exposures:
  - name: simple exposure spaced!!
    type: dashboard
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
"""

# deprecated test config fixtures
data_tests_yaml = """
models:
  - name: model
    columns:
     - name: id
       data_tests:
         - not_null
"""

test_type_mixed_yaml = """
models:
  - name: model
    columns:
     - name: id
       data_tests:
         - not_null
       tests:
         - unique
"""

old_tests_yml = """
models:
  - name: model
    tests:
      - custom
    columns:
      - name: id
        tests:
          - not_null

  - name: versioned_model
    tests:
      - custom
    versions:
      - v: 1
        tests:
        columns:
          - name: id
            tests:
              - not_null
"""

sources_old_tests_yaml = """
sources:
  - name: seed_source
    schema: "{{ var('schema_override', target.schema) }}"
    tables:
      - name: "seed"
        tests:
          - custom
        columns:
          - name: id
            tests:
              - unique
"""

seed_csv = """id,name
1,Mary
2,Sam
3,John
"""


local_dependency__dbt_project_yml = """

name: 'local_dep'
version: '1.0'

seeds:
  quote_columns: False

"""

local_dependency__schema_yml = """
sources:
  - name: seed_source
    schema: "{{ var('schema_override', target.schema) }}"
    tables:
      - name: "seed"
        columns:
          - name: id
            tests:
              - unique
"""

local_dependency__seed_csv = """id,name
1,Mary
2,Sam
3,John
"""
