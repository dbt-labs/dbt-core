import pytest

from dbt.tests.util import run_dbt

warnings_sql = """
{% do exceptions.warn('warning: everything is terrible but not that terrible') %}
{{ exceptions.warn("warning: everything is terrible but not that terrible") }}
select 1 as id
"""


class TestEmitWarning:
    @pytest.fixture(scope="class")
    def models(self):
        return {"warnings.sql": warnings_sql}

    def test_warn(self, project):
        run_dbt(["run"], expect_pass=True)
