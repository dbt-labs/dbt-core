SEED__CHIPMUNKS = """
name,shirt
alvin,red
simon,blue
theodore,green
dave,
""".strip()


MODEL__CHIPMUNKS = """
{{ config(materialized='table') }}
select *
from {{ ref('chipmunks_stage') }}
"""


TEST__VIEW_TRUE = """
{{ config(store_failures_as="view", store_failures=True) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__VIEW_FALSE = """
{{ config(store_failures_as="view", store_failures=False) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__VIEW_UNSET = """
{{ config(store_failures_as="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__TABLE_TRUE = """
{{ config(store_failures_as="table", store_failures=True) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__TABLE_FALSE = """
{{ config(store_failures_as="table", store_failures=False) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__TABLE_UNSET = """
{{ config(store_failures_as="table") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__EPHEMERAL_TRUE = """
{{ config(store_failures_as="ephemeral", store_failures=True) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__EPHEMERAL_FALSE = """
{{ config(store_failures_as="ephemeral", store_failures=False) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__EPHEMERAL_UNSET = """
{{ config(store_failures_as="ephemeral") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__UNSET_TRUE = """
{{ config(store_failures=True) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__UNSET_FALSE = """
{{ config(store_failures=False) }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__UNSET_UNSET = """
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__VIEW_UNSET_PASS = """
{{ config(store_failures_as="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'purple'
"""


TEST__ERROR_UNSET = """
{{ config(store_failures_as="error") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


SCHEMA_YML = """
version: 2

models:
  - name: chipmunks
    columns:
      - name: name
        data_tests:
          - not_null:
              store_failures_as: view
          - accepted_values:
              store_failures: false
              store_failures_as: table
              values:
                - alvin
                - simon
                - theodore
      - name: shirt
        data_tests:
          - not_null:
              store_failures: true
              store_failures_as: view
"""
