import pytest

from dbt.exceptions import DbtRuntimeError, TargetNotFoundError
from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.functional.compile.fixtures import (
    first_model_sql,
    second_model_sql,
    first_ephemeral_model_sql,
    second_ephemeral_model_sql,
    third_ephemeral_model_sql,
    schema_yml,
)


def get_lines(model_name):
    from dbt.tests.util import read_file

    f = read_file("target", "compiled", "test", "models", model_name + ".sql")
    return [line for line in f.splitlines() if line]


def file_exists(model_name):
    from dbt.tests.util import file_exists

    return file_exists("target", "compiled", "test", "models", model_name + ".sql")


class TestIntrospectFlag:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_model.sql": first_model_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": schema_yml,
        }

    def test_default(self, project):
        run_dbt(["compile"])
        assert get_lines("first_model") == ["select 1 as fun"]
        assert any("_test_compile as schema" in line for line in get_lines("second_model"))

    def test_no_introspect(self, project):
        with pytest.raises(DbtRuntimeError):
            run_dbt(["compile", "--no-introspect"])


class TestEphemeralModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_ephemeral_model.sql": first_ephemeral_model_sql,
            "second_ephemeral_model.sql": second_ephemeral_model_sql,
            "third_ephemeral_model.sql": third_ephemeral_model_sql,
        }

    def test_first_selector(self, project):
        run_dbt(["compile", "--select", "first_ephemeral_model"])
        assert file_exists("first_ephemeral_model")
        assert not file_exists("second_ephemeral_model")
        assert not file_exists("third_ephemeral_model")

    def test_middle_selector(self, project):
        run_dbt(["compile", "--select", "second_ephemeral_model"])
        assert file_exists("first_ephemeral_model")
        assert file_exists("second_ephemeral_model")
        assert not file_exists("third_ephemeral_model")

    def test_last_selector(self, project):
        run_dbt(["compile", "--select", "third_ephemeral_model"])
        assert file_exists("first_ephemeral_model")
        assert file_exists("second_ephemeral_model")
        assert file_exists("third_ephemeral_model")

    def test_no_selector(self, project):
        run_dbt(["compile"])

        assert get_lines("first_ephemeral_model") == ["select 1 as fun"]
        assert get_lines("second_ephemeral_model") == [
            "with __dbt__cte__first_ephemeral_model as (",
            "select 1 as fun",
            ")select * from __dbt__cte__first_ephemeral_model",
        ]
        assert get_lines("third_ephemeral_model") == [
            "with __dbt__cte__first_ephemeral_model as (",
            "select 1 as fun",
            "),  __dbt__cte__second_ephemeral_model as (",
            "select * from __dbt__cte__first_ephemeral_model",
            ")select * from __dbt__cte__second_ephemeral_model",
            "union all",
            "select 2 as fun",
        ]


class TestCompile:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_model.sql": first_model_sql,
            "second_model.sql": second_model_sql,
            "schema.yml": schema_yml,
        }

    def test_none(self, project):
        (results, log_output) = run_dbt_and_capture(["compile"])
        assert len(results) == 4
        assert "Compiled node" not in log_output

    def test_inline_pass(self, project):
        (results, log_output) = run_dbt_and_capture(
            ["compile", "--inline", "select * from {{ ref('first_model') }}"]
        )
        assert len(results) == 1
        assert "Compiled node 'inline_query' is:" in log_output

    def test_select_pass(self, project):
        (results, log_output) = run_dbt_and_capture(["compile", "--select", "second_model"])
        assert len(results) == 3
        assert "Compiled node 'second_model' is:" in log_output

    def test_select_pass_empty(self, project):
        (results, log_output) = run_dbt_and_capture(
            ["compile", "--indirect-selection", "empty", "--select", "second_model"]
        )
        assert len(results) == 1
        assert "Compiled node 'second_model' is:" in log_output

    def test_inline_fail(self, project):
        with pytest.raises(
            TargetNotFoundError, match="depends on a node named 'third_model' which was not found"
        ):
            run_dbt(["compile", "--inline", "select * from {{ ref('third_model') }}"])
