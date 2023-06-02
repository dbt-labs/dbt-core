import pytest

from dbt.contracts.results import RunStatus, TestStatus
from dbt.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt, write_file
from tests.functional.retry.fixtures import (
    models__sample_model,
    models__union_model,
    schema_yml,
    models__second_model,
)


class TestRetry:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": models__sample_model,
            "second_model.sql": models__second_model,
            "union_model.sql": models__union_model,
            "schema.yml": schema_yml,
        }

    def test_no_previous_run(self, project):
        with pytest.raises(
            DbtRuntimeError, match="Could not find previous run in 'target' target directory"
        ):
            run_dbt(["retry"])

        with pytest.raises(
            DbtRuntimeError, match="Could not find previous run in 'walmart' target directory"
        ):
            run_dbt(["retry", "--state", "walmart"])

    def test_previous_run(self, project):
        # Regular build
        results = run_dbt(["build"], expect_pass=False)

        expected_statuses = {
            "sample_model": RunStatus.Error,
            "second_model": RunStatus.Success,
            "union_model": RunStatus.Skipped,
            "accepted_values_sample_model_foo__False__3": RunStatus.Skipped,
            "accepted_values_second_model_bar__False__3": TestStatus.Warn,
            "accepted_values_union_model_sum3__False__3": RunStatus.Skipped,
        }

        assert {n.node.name: n.status for n in results.results} == expected_statuses

        # Ignore second_model which succeeded
        results = run_dbt(["retry"], expect_pass=False)

        expected_statuses = {
            "sample_model": RunStatus.Error,
            "union_model": RunStatus.Skipped,
            "accepted_values_union_model_sum3__False__3": RunStatus.Skipped,
            "accepted_values_sample_model_foo__False__3": RunStatus.Skipped,
        }

        assert {n.node.name: n.status for n in results.results} == expected_statuses

        # Fix sample model and retry, everything should pass
        fixed_sql = "select 1 as id, 1 as foo"
        write_file(fixed_sql, "models", "sample_model.sql")

        results = run_dbt(["retry"])

        expected_statuses = {
            "sample_model": RunStatus.Success,
            "union_model": RunStatus.Success,
            "accepted_values_union_model_sum3__False__3": TestStatus.Pass,
            "accepted_values_sample_model_foo__False__3": TestStatus.Warn,
        }

        assert {n.node.name: n.status for n in results.results} == expected_statuses

        # No failures in previous run, nothing to retry
        results = run_dbt(["retry"])
        expected_statuses = {}
        assert {n.node.name: n.status for n in results.results} == expected_statuses

    def test_warn_error(self, project):
        # Regular build
        results = run_dbt(["--warn-error", "build", "--select", "second_model"], expect_pass=False)

        expected_statuses = {
            "second_model": RunStatus.Success,
            "accepted_values_second_model_bar__False__3": TestStatus.Fail,
        }

        assert {n.node.name: n.status for n in results.results} == expected_statuses

        # Retry regular, should pass
        run_dbt(["retry"])

        # Retry with --warn-error, should fail
        run_dbt(["--warn-error", "retry"], expect_pass=False)
