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


class TestSourceQuotingGlobalConfigs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Postgres quoting configs are True by default -- turn them all to False to show they are not respected during source rendering
        return {
            "quoting": {
                "database": False,
                "schema": False,
                "identifier": False,
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
        assert generated_sql == 'select * from "source_database"."source_schema"."customers"'


class TestModelQuoting:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Postgres quoting configs are True by default -- turn them all to False to show they are respected during model rendering
        return {
            "quoting": {
                "database": False,
                "schema": False,
                "identifier": False,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": "select 1 as id",
            "model_downstream.sql": "select * from {{ ref('model') }}",
        }

    def test_models_respect_global_quoting_configs(self, project):
        run_dbt(["compile"])

        generated_sql = read_file("target", "compiled", "test", "models", "model_downstream.sql")
        assert generated_sql == f"select * from dbt.{project.test_schema}.model"
