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


deprecated_model_exposure_yaml = """
version: 2

models:
  - name: model
    deprecation_date: 1999-01-01 00:00:00.00+00:00

exposures:
  - name: simple_exposure
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


invalid_deprecation_date_yaml = """
models:
  - name: models_trivial
    description: "This is a test model"
    deprecation_date: 1
"""

duplicate_keys_yaml = """
models:
  - name: models_trivial
    description: "This is a test model"
    deprecation_date: 1999-01-01 00:00:00.00+00:00

models:
  - name: models_trivial
    description: "This is a test model"
    deprecation_date: 1999-01-01 00:00:00.00+00:00
"""

custom_key_in_config_yaml = """
models:
  - name: models_trivial
    description: "This is a test model"
    deprecation_date: 1999-01-01 00:00:00.00+00:00
    config:
      my_custom_key: "my_custom_value"
"""

multiple_custom_keys_in_config_yaml = """
models:
  - name: models_trivial
    description: "This is a test model"
    deprecation_date: 1999-01-01 00:00:00.00+00:00
    config:
      my_custom_key: "my_custom_value"
      my_custom_key2: "my_custom_value2"
"""

custom_key_in_object_yaml = """
models:
  - name: models_trivial
    description: "This is a test model"
    deprecation_date: 1999-01-01 00:00:00.00+00:00
    my_custom_property: "It's over, I have the high ground"
"""

property_moved_to_config_yaml = """
models:
  - name: models_trivial
    description: "This is a test model"
    access: public  # deprecated - should be in config
    columns:
      - name: id
        tags: ["test"]  # deprecated - should be in config

sources:
  - name: seed_source
    schema: "{{ var('schema_override', target.schema) }}"
    tags: ["test"]  # deprecated - should be in config
    tables:
      - name: "seed"
        tags: ["test"]  # deprecated - should be in config
        columns:
          - name: id
            tags: ["test"]  # deprecated - should be in config
"""

test_with_arguments_yaml = """
models:
  - name: models_trivial
    tests:
      - test_name: unique
        arguments:
          custom: arg
      - custom_test:
          arguments:
            custom: arg
      - unique
      - not_null:
          where: "1=1"
"""


test_with_arguments_yaml = """
models:
  - name: models_trivial
    tests:
      - test_name: unique
        arguments:
          custom: arg
      - custom_test:
          arguments:
            custom: arg
      - unique
      - not_null:
          where: "1=1"
    columns:
      - name: column
        tests:
          - test_name: unique
            arguments:
              custom: arg
          - custom_test:
              arguments:
                custom: arg
          - custom_test2_valid:
              column_name: id
              config:
                where: "1=1"
              custom: arg
"""


test_missing_arguments_property_yaml = """
models:
  - name: models_trivial
    tests:
      - test_name: unique
        custom: arg
      - custom_test:
          custom: arg
      - custom_test2_valid:
          column_name: id
          config:
            where: "1=1"
          arguments:
            custom: arg
    columns:
      - name: column
        tests:
          - test_name: unique
            custom: arg
          - custom_test:
              custom: arg
          - custom_test2_valid:
              column_name: id
              config:
                where: "1=1"
              arguments:
                custom: arg
      """
