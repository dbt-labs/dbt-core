import pytest
from dbt.tests.util import read_file


schema_yml = """
version: 2
models:
  - name: emails
    columns:
    - name: email
      tests:
      - not_null:
          severity: warn
  - name: users
    columns:
    - name: id
      tests:
      - unique
  - name: users_rollup
    columns:
    - name: gender
      tests:
      - unique

sources:
  - name: raw
    schema: '{{ target.schema }}'
    tables:
      - name: seed

exposures:
  - name: user_exposure
    type: dashboard
    depends_on:
      - ref('users')
      - ref('users_rollup')
    owner:
      email: nope@example.com
  - name: seed_ml_exposure
    type: ml
    depends_on:
      - source('raw', 'seed')
    owner:
      email: nope@example.com
"""

base_users_sql = """

{{
    config(
        materialized = 'ephemeral',
        tags = ['base']
    )
}}

select * from {{ source('raw', 'seed') }}
"""

users_sql = """

{{
    config(
        materialized = 'table',
        tags=['bi', 'users']
    )
}}

select * from {{ ref('base_users') }}
"""

users_rollup_sql = """

{{
    config(
        materialized = 'view',
        tags = 'bi'
    )
}}

with users as (

    select * from {{ ref('users') }}

)

select
    gender,
    count(*) as ct
from users
group by 1
"""

users_rollup_dependency_sql = """
{{
  config(materialized='table')
}}

select * from {{ ref('users_rollup') }}
"""

emails_sql = """

{{
    config(materialized='ephemeral', tags=['base'])
}}

select distinct email from {{ ref('base_users') }}
"""

emails_alt_sql = """
select distinct email from {{ ref('users') }}
"""

alternative_users_sql = """
{# Same as ´users´ model, but with dots in the model name #}
{{
    config(
        materialized = 'table',
        tags=['dots']
    )
}}

select * from {{ ref('base_users') }}
"""

never_selected_sql = """
{{
  config(schema='_and_then')
}}

select * from {{ this.schema }}.seed
"""

subdir_sql = """
select 1 as id
"""

nested_users_sql = """
select 1 as id
"""

properties_yml = """
version: 2
seeds:
  - name: summary_expected
    config:
      column_types:
        ct: BIGINT
        gender: text
"""


class SelectionFixtures:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "base_users.sql": base_users_sql,
            "users.sql": users_sql,
            "users_rollup.sql": users_rollup_sql,
            "users_rollup_dependency.sql": users_rollup_dependency_sql,
            "emails.sql": emails_sql,
            "emails_alt.sql": emails_alt_sql,
            "alternative.users.sql": alternative_users_sql,
            "never_selected.sql": never_selected_sql,
            "test": {
                "subdir.sql": subdir_sql,
                "subdir": {"nested_users.sql": nested_users_sql},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        # Read seed file and return
        seeds = {"properties.yml": properties_yml}
        for seed_file in ["seed.csv", "summary_expected.csv"]:
            seeds[seed_file] = read_file(test_data_dir, seed_file)
        return seeds
