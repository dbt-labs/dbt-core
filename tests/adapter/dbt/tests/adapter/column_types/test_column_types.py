import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.column_types.fixtures import macro_test_is_type_sql, model_sql, schema_yml


class BaseColumnTypes:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_is_type.sql": macro_test_is_type_sql}

    def run_and_test(self):
        results = run_dbt(["run"])
        assert len(results) == 1
        results = run_dbt(["test"])
        assert len(results) == 1


class TestPostgresColumnTypes(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql, "schema.yml": schema_yml}

    def test_run_and_test(self, project):
        self.run_and_test()
