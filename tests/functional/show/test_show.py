import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt_and_capture, run_dbt
from tests.functional.show.fixtures import (
    BaseConfigProject,
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
        assert "sample_num" in log_output
        assert "sample_bool" in log_output
