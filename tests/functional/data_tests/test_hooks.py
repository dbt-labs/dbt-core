from unittest import mock

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt_common.exceptions import CompilationError

orders_csv = """order_id,order_date,customer_id
1,2024-06-01,1001
2,2024-06-02,1002
3,2024-06-03,1003
4,2024-06-04,1004
"""


orders_model_sql = """
with source as (
    select
        order_id,
        order_date,
        customer_id
    from {{ ref('seed_orders') }}
),
final as (
    select
        order_id,
        order_date,
        customer_id
    from source
)
select * from final
"""


orders_test_sql = """
select *
from {{ ref('orders') }}
where order_id is null
"""


class BaseSingularTestHooks:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_orders.csv": orders_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"orders.sql": orders_model_sql}

    @pytest.fixture(scope="class")
    def tests(self):
        return {"orders_test.sql": orders_test_sql}


class TestSingularTestPreHook(BaseSingularTestHooks):
    def test_data_test_runs_adapter_pre_hook_pass(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 1

        mock_pre_model_hook = mock.Mock()
        with mock.patch.object(type(project.adapter), "pre_model_hook", mock_pre_model_hook):
            results = run_dbt(["test"], expect_pass=True)
            assert len(results) == 1
            mock_pre_model_hook.assert_called_once()

    def test_data_test_runs_adapter_pre_hook_fails(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 1

        mock_pre_model_hook = mock.Mock()
        mock_pre_model_hook.side_effect = CompilationError("exception from adapter.pre_model_hook")
        with mock.patch.object(type(project.adapter), "pre_model_hook", mock_pre_model_hook):
            (_, log_output) = run_dbt_and_capture(["test"], expect_pass=False)
            assert "exception from adapter.pre_model_hook" in log_output


class TestSingularTestPostHook(BaseSingularTestHooks):
    def test_data_test_runs_adapter_post_hook_pass(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 1

        mock_post_model_hook = mock.Mock()
        with mock.patch.object(type(project.adapter), "post_model_hook", mock_post_model_hook):
            results = run_dbt(["test"], expect_pass=True)
            assert len(results) == 1
            mock_post_model_hook.assert_called_once()

    def test_data_test_runs_adapter_post_hook_fails(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 1

        mock_post_model_hook = mock.Mock()
        mock_post_model_hook.side_effect = CompilationError(
            "exception from adapter.post_model_hook"
        )
        with mock.patch.object(type(project.adapter), "post_model_hook", mock_post_model_hook):
            (_, log_output) = run_dbt_and_capture(["test"], expect_pass=False)
            assert "exception from adapter.post_model_hook" in log_output
