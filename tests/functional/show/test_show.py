import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt_and_capture, run_dbt
from tests.functional.show.fixtures import (
    BaseConfigProject,
    models__second_ephemeral_model,
)


class TestShow(BaseConfigProject):
    def test_none(self, project):
        with pytest.raises(
            DbtRuntimeError, match="Either --select or --inline must be passed to show"
        ):
            run_dbt(["deps"])
            run_dbt(["show"])

    def test_select_model_text(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "second_model"])
        assert "Previewing node 'sample_model'" not in log_output
        assert "Previewing node 'second_model'" in log_output
        assert "col_one" in log_output
        assert "col_two" in log_output
        assert "answer" in log_output

    def test_select_multiple_model_text(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model second_model"]
        )
        assert "Previewing node 'sample_model'" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_select_single_model_json(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model", "--output", "json"]
        )
        assert "Previewing node 'sample_model'" not in log_output
        assert '"sample_num"' in log_output
        assert '"sample_bool"' in log_output

    def test_select_overflow_limit(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model", "--limit", "6"]
        )
        assert "Previewing node 'sample_model'" in log_output
        assert "6" in log_output

    def test_select_negative_limit(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--select", "sample_model", "--limit", "-1"]
        )
        assert "7" in log_output

    def test_inline_pass(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--inline", "select * from {{ ref('sample_model') }}"]
        )
        assert "Previewing inline node" in log_output
        assert "sample_num" in log_output
        assert "sample_bool" in log_output

    def test_inline_fail(self, project):
        run_dbt(["deps"])
        with pytest.raises(
            DbtRuntimeError, match="depends on a node named 'third_model' which was not found"
        ):
            run_dbt(["show", "--inline", "select * from {{ ref('third_model') }}"])

    def test_ephemeral_model(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(["show", "--select", "ephemeral_model"])
        assert "col_deci" in log_output

    def test_second_ephemeral_model(self, project):
        run_dbt(["deps"])
        (results, log_output) = run_dbt_and_capture(
            ["show", "--inline", models__second_ephemeral_model]
        )
        assert "col_hundo" in log_output
