my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_wrong_order_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as id,
  cast('2019-01-01' as date) as date_day
"""

my_model_wrong_name_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as error,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_data_type_sql = """
{{{{
  config(
    materialized = "table"
  )
}}}}

select
  {sql_value} as wrong_data_type_column_name
"""

my_model_with_nulls_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  -- null value for 'id'
  cast(null as {{ dbt.type_int() }}) as id,
  -- change the color as well (to test rollback)
  'red' as color,
  cast('2019-01-01' as date) as date_day
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract: true
    columns:
      - name: id
        quote: true
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        constraints_check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
  - name: my_model_error
    config:
      contract: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        constraints_check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
  - name: my_model_wrong_order
    config:
      contract: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        constraints_check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
  - name: my_model_wrong_name
    config:
      contract: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        constraints_check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
"""

model_data_type_schema_yml = """
version: 2
models:
  - name: my_model_data_type
    config:
      contract: true
    columns:
      - name: wrong_data_type_column_name
        data_type: {data_type}
"""
