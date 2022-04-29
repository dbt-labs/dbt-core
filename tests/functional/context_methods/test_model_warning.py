import pytest

from dbt.tests.util import run_dbt

warnings_sql = """
{% do exceptions.warn('warning: everything is terrible but not that terrible') %}
{{ exceptions.warn("warning: everything is terrible but not that terrible") }}
select 1 as id
"""

# Note: this test is in the wrong directory. It has nothing to do with contexts,
# and is just a test that issuing a warning in a model works. Move when a better
# location is identified.


class TestEmitWarning:
    @pytest.fixture(scope="class")
    def models(self):
        return {"warnings.sql": warnings_sql}

    def test_warn(self, project):
        run_dbt(["run"], expect_pass=True)
