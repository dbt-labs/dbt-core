import pytest

from dbt.contracts.results import RunResult
from dbt.tests.util import run_dbt


models__one_sql = """
select 1 /failed
"""

models__two_sql = """
select 1 /failed
"""


class FailFastBase:
    @pytest.fixture(scope="class")
    def models(self):
        return {"one.sql": models__one_sql, "two.sql": models__two_sql}


class TestFastFailingDuringRun(FailFastBase):
    def test_fail_fast_run(
        self,
        project,
        models,  # noqa: F811
    ):
        res = run_dbt(["run", "--fail-fast", "--threads", "1"], expect_pass=False)
        # a RunResult contains only one node so we can be sure only one model was run
        assert type(res) == RunResult


class TestFailFastFromConfig(FailFastBase):
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "config": {
                "send_anonymous_usage_stats": False,
                "fail_fast": True,
            }
        }

    def test_fail_fast_run_user_config(
        self,
        project,
        models,  # noqa: F811
    ):
        res = run_dbt(["run", "--threads", "1"], expect_pass=False)
        # a RunResult contains only one node so we can be sure only one model was run
        assert type(res) == RunResult
