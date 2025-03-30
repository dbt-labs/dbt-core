model_foreign_key_model_schema_yml = """
models:
  - name: my_model
    constraints:
      - type: foreign_key
        columns: [id]
        to: ref('my_model_to')
        to_columns: [id]
    columns:
      - name: id
        data_type: integer
"""


model_foreign_key_source_schema_yml = """
sources:
  - name: test_source
    tables:
      - name: test_table

models:
  - name: my_model
    constraints:
      - type: foreign_key
        columns: [id]
        to: source('test_source', 'test_table')
        to_columns: [id]
    columns:
      - name: id
        data_type: integer
"""


model_foreign_key_model_node_not_found_schema_yml = """
models:
  - name: my_model
    constraints:
      - type: foreign_key
        columns: [id]
        to: ref('doesnt_exist')
        to_columns: [id]
    columns:
      - name: id
        data_type: integer
"""


model_foreign_key_model_invalid_syntax_schema_yml = """
models:
  - name: my_model
    constraints:
      - type: foreign_key
        columns: [id]
        to: invalid
        to_columns: [id]
    columns:
      - name: id
        data_type: integer
"""


model_foreign_key_model_column_schema_yml = """
models:
  - name: my_model
    columns:
      - name: id
        data_type: integer
        constraints:
        - type: foreign_key
          to: ref('my_model_to')
          to_columns: [id]
"""


model_foreign_key_column_invalid_syntax_schema_yml = """
models:
  - name: my_model
    columns:
      - name: id
        data_type: integer
        constraints:
        - type: foreign_key
          to: invalid
          to_columns: [id]
"""


model_foreign_key_column_node_not_found_schema_yml = """
models:
  - name: my_model
    columns:
      - name: id
        data_type: integer
        constraints:
        - type: foreign_key
          to: ref('doesnt_exist')
          to_columns: [id]
"""

model_column_level_foreign_key_source_schema_yml = """
sources:
  - name: test_source
    tables:
      - name: test_table

models:
  - name: my_model
    columns:
      - name: id
        data_type: integer
        constraints:
        - type: foreign_key
          to: source('test_source', 'test_table')
          to_columns: [id]
"""
