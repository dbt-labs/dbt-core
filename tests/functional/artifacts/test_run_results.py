import pytest
from dbt.tests.util import run_dbt

good_model_sql = """
select 1 as id
"""

bad_model_sql = """
something bad
"""


class TestRunResultsTimingSuccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": good_model_sql}

    def test_timing_exists(self, project):
        results = run_dbt(["run"])
        assert results.results[0].timing


class TestRunResultsTimingFailure:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": bad_model_sql}

    def test_timing_exists(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert results.results[0].timing
