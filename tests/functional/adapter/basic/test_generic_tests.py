import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.functional.adapter.basic.files import (
    base_table_sql,
    base_view_sql,
    generic_test_seed_yml,
    generic_test_table_yml,
    generic_test_view_yml,
    schema_base_yml,
    seeds_base_csv,
)


class BaseGenericTests:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "schema.yml": generic_test_seed_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "schema.yml": schema_base_yml,
            "schema_view.yml": generic_test_view_yml,
            "schema_table.yml": generic_test_table_yml,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_generic_tests(self, project):
        # seed command
        results = run_dbt(["seed"])

        # test command selecting base model
        results = run_dbt(["test", "-m", "base"])
        assert len(results) == 1

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2

        # test command, all tests
        results, log_output = run_dbt_and_capture(["test", "--log-format", "json"])
        assert len(results) == 3

        result_log_lines = [
            line for line in log_output.split("\n") if "LogTestResult" in line and "group" in line
        ]
        assert len(result_log_lines) == 1
        assert "my_group" in result_log_lines[0]
        assert "group_owner" in result_log_lines[0]
        assert "model.generic_tests.view_model" in result_log_lines[0]


class TestGenericTests(BaseGenericTests):
    pass
