import pytest

from dbt.tests.util import run_dbt, get_connection


class BaseDropSchemaNamed:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": "select 1 as id",
        }

    def test_dropped_schema_named_drops_expected_schema(self, project):

        results = run_dbt(["run"])
        assert len(results) == 1

        run_dbt(
            [
                "run-operation",
                "drop_schema_named",
                "--args",
                f"{{schema_name: {project.test_schema} }}",
            ]
        )

        adapter = project.adapter
        with get_connection(adapter):
            schemas = adapter.list_schemas(project.database)

        assert project.test_schema not in schemas


class TestDropSchemaNamed(BaseDropSchemaNamed):
    pass
