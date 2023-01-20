models_sql = """
select 1 as id
"""

second_model_sql = """
select 1 as id
"""


source_schema_yml = """version: 2

sources:
  - name: test_source
    tables:
      - name: test_table
"""

metrics_schema_yml = """version: 2

metrics:
  - name: metric
    model: ref('model')
    label: "label"

    calculation_method: count_distinct
    expression: id

    timestamp: first_order
    time_grains: [day]
"""

simple_exposure_yml = """
version: 2

exposures:
  - name: simple_exposure
    label: simple exposure label
    type: dashboard
    depends_on:
      - ref('model')
      - source('test_source', 'test_table')
      - metric('metric')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""

disabled_models_exposure_yml = """
version: 2

exposures:
  - name: simple_exposure
    type: dashboard
    config:
      enabled: False
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""

enabled_yaml_level_exposure_yml = """
version: 2

exposures:
  - name: simple_exposure
    type: dashboard
    config:
      enabled: True
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""

invalid_config_exposure_yml = """
version: 2

exposures:
  - name: simple_exposure
    type: dashboard
    config:
      enabled: True and False
    depends_on:
      - ref('model')
    owner:
      email: something@example.com
"""
