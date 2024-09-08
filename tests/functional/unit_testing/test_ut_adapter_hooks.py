from unittest import mock

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt_common.exceptions import CompilationError
from tests.functional.unit_testing.fixtures import (
    my_model_a_sql,
    my_model_b_sql,
    my_model_sql,
    test_my_model_pass_yml,
)


class BaseUnitTestAdapterHook:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_pass_yml,
        }


class TestUnitTestAdapterPreHook(BaseUnitTestAdapterHook):
    def test_unit_test_runs_adapter_pre_hook_passes(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        mock_pre_model_hook = mock.Mock()
        with mock.patch.object(type(project.adapter), "pre_model_hook", mock_pre_model_hook):
            results = run_dbt(["test", "--select", "test_name:test_my_model"], expect_pass=True)

            assert len(results) == 1
            mock_pre_model_hook.assert_called_once()

    def test_unit_test_runs_adapter_pre_hook_fails(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        mock_pre_model_hook = mock.Mock()
        mock_pre_model_hook.side_effect = CompilationError("exception from adapter.pre_model_hook")
        with mock.patch.object(type(project.adapter), "pre_model_hook", mock_pre_model_hook):
            (_, log_output) = run_dbt_and_capture(
                ["test", "--select", "test_name:test_my_model"], expect_pass=False
            )
            assert "exception from adapter.pre_model_hook" in log_output


class TestUnitTestAdapterPostHook(BaseUnitTestAdapterHook):
    def test_unit_test_runs_adapter_post_hook_pass(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        mock_post_model_hook = mock.Mock()
        with mock.patch.object(type(project.adapter), "post_model_hook", mock_post_model_hook):
            results = run_dbt(["test", "--select", "test_name:test_my_model"], expect_pass=True)

            assert len(results) == 1
            mock_post_model_hook.assert_called_once()

    def test_unit_test_runs_adapter_post_hook_fails(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        mock_post_model_hook = mock.Mock()
        mock_post_model_hook.side_effect = CompilationError(
            "exception from adapter.post_model_hook"
        )
        with mock.patch.object(type(project.adapter), "post_model_hook", mock_post_model_hook):
            (_, log_output) = run_dbt_and_capture(
                ["test", "--select", "test_name:test_my_model"], expect_pass=False
            )
            assert "exception from adapter.post_model_hook" in log_output
