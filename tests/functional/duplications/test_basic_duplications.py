import pytest
from dbt.tests.util import run_dbt_and_capture

duplicate_key_schema__schema_yml = """
version: 2
models:
  - name: my_model
models:
  - name: my_model
"""

my_model_sql = """
  select 1 as fun
"""


@pytest.fixture(scope="class")
def models():
    return {
        "my_model.sql": my_model_sql,
        "schema.yml": duplicate_key_schema__schema_yml,
    }


def test_duplicate_key_in_yaml(project):
    results, stdout = run_dbt_and_capture(["run"])
    assert "Duplicate 'models' key found in yaml file models/schema.yml" in stdout
