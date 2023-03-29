import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt_and_capture, run_dbt
from tests.functional.show.fixtures import (
    models__second_ephemeral_model,
    seeds__sample_seed,
    models__sample_model,
    models__second_model,
    models__ephemeral_model,
)


class TestShow:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": models__sample_model,
            "second_model.sql": models__second_model,
            "ephemeral_model.sql": models__ephemeral_model,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"sample_seed.csv": seeds__sample_seed}

    def test_none(self, project):
        with pytest.raises(
            DbtRuntimeError, match="Either --select or --inline must be passed to show"
        ):
            run_dbt(["seed"])
            run_dbt(["show"])

    def test_select_model_text(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "second_model"])
        assert "Previewing node 'sample_model'" not in log_output
        assert "Previewing node 'second_model'" in log_output
        assert "col_one" in log_output
        assert "col_two" in log_output
        assert "answer" in log_output

    def test_select_multiple_model_text(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model second_model"]
        )
        assert "Previewing node 'sample_model'" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_select_single_model_json(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model", "--output", "json"]
        )
        assert "Previewing node 'sample_model'" not in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_inline_pass(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--inline", "select * from {{ ref('sample_model') }}"]
        )
        assert "Previewing inline node" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_inline_fail(self, project):
        run_dbt(["build"])
        with pytest.raises(
            DbtRuntimeError, match="depends on a node named 'third_model' which was not found"
        ):
            run_dbt(["show", "--inline", "select * from {{ ref('third_model') }}"])

    def test_ephemeral_model(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "ephemeral_model"])
        assert "col_deci" in log_output

    def test_second_ephemeral_model(self, project):
        run_dbt(["build"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--inline", models__second_ephemeral_model]
        )
        assert "col_hundo" in log_output
