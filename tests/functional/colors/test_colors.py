import re

import pytest

from dbt.tests.util import run_dbt_and_capture

models__do_nothing_then_fail_sql = """
select 1,

"""


@pytest.fixture(scope="class")
def models():
    return {"do_nothing_then_fail.sql": models__do_nothing_then_fail_sql}


@pytest.fixture(scope="class")
def project_config_update():
    return {"config-version": 2}


class TestColors:
    def test_use_colors(self, project):
        self.assert_colors_used(
            "--use-colors",
            expect_colors=True,
        )

    def test_no_use_colors(self, project):
        self.assert_colors_used(
            "--no-use-colors",
            expect_colors=False,
        )

    def assert_colors_used(self, flag, expect_colors):
        _, stdout = run_dbt_and_capture(args=[flag, "run"], expect_pass=False)
        # pattern to match formatted log output
        pattern = re.compile(r"\[31m.*|\[33m.*")
        stdout_contains_formatting_characters = bool(pattern.search(stdout))
        if expect_colors:
            assert stdout_contains_formatting_characters
        else:
            assert not stdout_contains_formatting_characters
