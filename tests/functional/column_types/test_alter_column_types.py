import pytest
from dbt.tests.util import run_dbt
from tests.functional.column_types.fixtures import (
    macro_test_alter_column_type,
    macro_test_is_type_sql,
    schema_yml,
)


class TestAlterColumnTypes:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "test_alter_column_type.sql": macro_test_alter_column_type,
            "test_is_type.sql": macro_test_is_type_sql,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
        }

    def test_run_alter_and_test(self, project):
        kwargs = "model_name, column_name, new_column_type"
        results = run_dbt(["run"])
        assert len(results) == 1
        run_dbt(["run-operation", "test_alter_column_type" "--args", kwargs])
        results = run_dbt(["test"])
        assert len(results) == 1
