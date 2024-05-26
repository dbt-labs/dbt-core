import pytest

from dbt.tests.util import run_dbt

models_naming_dupes_schema_yml = """
version: 2
models:
  - name: something
    description: This table has basic information about orders, as well as some derived facts based on payments
exposure:
  - name: something

"""

something_model_sql = """

select 1 as item

"""


class TestDuplicateSchemaResource:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_naming_dupes_schema_yml,
            "something.sql": something_model_sql,
        }

    # a model and an exposure can share the same name
    def test_duplicate_model_and_exposure(self, project):
        result = run_dbt(["compile"])
        assert len(result) == 1
