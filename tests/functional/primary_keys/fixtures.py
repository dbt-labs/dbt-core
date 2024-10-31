simple_model_sql = """
select 1 as id, 'blue' as color
"""

simple_model_unique_test = """
models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - unique
"""

simple_model_disabled_unique_test = """
models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - unique:
              enabled: false

"""

simple_model_unique_not_null_tests = """
models:
  - name: simple_model
    columns:
      - name: id
        tests:
          - unique
          - not_null
"""

simple_model_unique_combo_of_columns = """
models:
  - name: simple_model
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [id, color]
"""

simple_model_constraints = """
models:
  - name: simple_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: primary_key
      - name: color
        data_type: text
"""

simple_model_two_versions_both_configured = """
models:
  - name: simple_model
    latest_version: 1
    columns:
      - name: id
        tests:
          - unique
          - not_null
    versions:
      - v: 1
      - v: 2
"""

simple_model_two_versions_exclude_col = """
models:
  - name: simple_model
    latest_version: 1
    columns:
      - name: id
        tests:
          - unique
          - not_null
    versions:
      - v: 1
      - v: 2
        columns:
          - include: all
            exclude: [id]
"""
