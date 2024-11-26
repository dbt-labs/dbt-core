import pytest

from dbt.contracts.results import RunStatus, TestStatus
from dbt.tests.util import run_dbt, write_file
from tests.functional.retry.fixtures import models__thread_model, schema_test_thread_yml


class TestCustomThreadRetry:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "thread_model.sql": models__thread_model,
            "schema.yml": schema_test_thread_yml,
        }

    def test_thread_target(self, project):
        # Passing Threads to check
        results = run_dbt(
            ["build", "--select", "thread_model", "--threads", "3"], expect_pass=False
        )
        expected_statuses = {
            "thread_model": RunStatus.Error,
            "not_null_thread_model_id": TestStatus.Skipped,
        }
        assert {n.node.name: n.status for n in results.results} == expected_statuses

        # Retry Running the Dbt with simple Retry
        results = run_dbt(["retry", "--threads", "2"], expect_pass=False)
        expected_statuses = {
            "thread_model": RunStatus.Error,
            "not_null_thread_model_id": TestStatus.Skipped,
        }
        assert {n.node.name: n.status for n in results.results} == expected_statuses
        assert results.args["threads"] == 2

        # running with retry withour threads
        results = run_dbt(["retry"], expect_pass=False)
        expected_statuses = {
            "thread_model": RunStatus.Error,
            "not_null_thread_model_id": TestStatus.Skipped,
        }
        assert {n.node.name: n.status for n in results.results} == expected_statuses
        assert results.args["threads"] == 2

        # Retry with fixing the model and running with --threads 1
        fixed_sql = "select 1 as id"
        write_file(fixed_sql, "models", "thread_model.sql")

        results = run_dbt(["retry", "--threads", "1"])
        expected_statuses = {
            "thread_model": RunStatus.Success,
            "not_null_thread_model_id": TestStatus.Pass,
        }

        assert {n.node.name: n.status for n in results.results} == expected_statuses
        assert results.args["threads"] == 1
