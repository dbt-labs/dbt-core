import pytest

from dbt.tests.util import run_dbt


model_one_sql = """
someting bad
"""


class TestHandledExit:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_one.sql": model_one_sql}

    def test_failed_run_does_not_throw(self, project):
        run_dbt(["run"], expect_pass=False)
