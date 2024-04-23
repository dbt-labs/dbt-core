import pytest

from dbt.tests.util import run_dbt

single_test_sql = """
{{ config(warn_if = '>0', error_if ="> 10") }}

select 1 as issue
"""


class TestSingularTestWarnError:
    @pytest.fixture(scope="class")
    def tests(self):
        return {"single_test.sql": single_test_sql}

    def test_singular_test_warn_error(self, project):
        results = run_dbt(["--warn-error", "test"], expect_pass=False)
        assert results.results[0].status == "fail"

    def test_singular_test_warn_error_options(self, project):
        results = run_dbt(
            ["--warn-error-options", "{'include': 'all'}", "test"], expect_pass=False
        )
        assert results.results[0].status == "fail"

    def test_singular_test_equals_warn_error(self, project):
        results = run_dbt(["--warn-error", "test"], expect_pass=False)
        warn_error_result = results.results[0].status

        results = run_dbt(
            ["--warn-error-options", "{'include': 'all'}", "test"], expect_pass=False
        )
        assert warn_error_result == results.results[0].status
