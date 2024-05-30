empty_configuration_yml = """
version: 2
models:
  - name: table
    columns:
      - name: color
        data_tests:
          - accepted_values:
              values: ['blue', 'red']
"""


custom_config_yml = """
version: 2
models:
  - name: table
    columns:
      - name: color
        tests:
          - accepted_values:
              values: ['blue', 'red']
              config:
                custom_config_key: some_value
"""

mixed_config_yml = """
version: 2
models:
  - name: table
    columns:
      - name: color
        tests:
          - accepted_values:
              values: ['blue', 'red']
              severity: warn
              config:
                custom_config_key: some_value
"""

same_key_error_yml = """
version: 2
models:
  - name: table
    columns:
      - name: color
        tests:
          - accepted_values:
              values: ['blue', 'red']
              severity: warn
              config:
                severity: error
"""

seed_csv = """
id,color,value
1,blue,10
2,red,20
3,green,30
4,yellow,40
5,blue,50
6,red,60
7,blue,70
8,green,80
9,yellow,90
10,blue,100

"""

table_sql = """
-- content of the table.sql
select * from {{ ref('seed') }}
"""
