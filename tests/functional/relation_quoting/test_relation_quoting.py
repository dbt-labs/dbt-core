import pytest

from dbt.tests.util import read_file, run_dbt

_SOURCES_YML = """
sources:
  - name: source_name
    database: source_database
    schema: source_schema
    tables:
      - name: customers
"""


class TestSourceQuotingLegacy:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "quoting": {
                "database": True,
                "schema": True,
                "database": True,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sources.yml": _SOURCES_YML,
            "model.sql": "select * from {{ source('source_name', 'customers') }}",
        }

    def test_sources_ignore_global_quoting_configs(self, project):
        run_dbt(["compile"])

        generated_sql = read_file("target", "compiled", "test", "models", "model.sql")
        assert generated_sql == "select * from source_database.source_schema.customers"