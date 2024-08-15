import pytest

from dbt.tests.util import run_dbt

models__do_nothing__sql = """
with x as (select pg_sleep(1)) select 1
"""


class TestThreadCount:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "do_nothing_1.sql": models__do_nothing__sql,
            "do_nothing_2.sql": models__do_nothing__sql,
            "do_nothing_3.sql": models__do_nothing__sql,
            "do_nothing_4.sql": models__do_nothing__sql,
            "do_nothing_5.sql": models__do_nothing__sql,
            "do_nothing_6.sql": models__do_nothing__sql,
            "do_nothing_7.sql": models__do_nothing__sql,
            "do_nothing_8.sql": models__do_nothing__sql,
            "do_nothing_9.sql": models__do_nothing__sql,
            "do_nothing_10.sql": models__do_nothing__sql,
            "do_nothing_11.sql": models__do_nothing__sql,
            "do_nothing_12.sql": models__do_nothing__sql,
            "do_nothing_13.sql": models__do_nothing__sql,
            "do_nothing_14.sql": models__do_nothing__sql,
            "do_nothing_15.sql": models__do_nothing__sql,
            "do_nothing_16.sql": models__do_nothing__sql,
            "do_nothing_17.sql": models__do_nothing__sql,
            "do_nothing_18.sql": models__do_nothing__sql,
            "do_nothing_19.sql": models__do_nothing__sql,
            "do_nothing_20.sql": models__do_nothing__sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2}

    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {"threads": 2}

    def test_threading_8x(self, project):
        results = run_dbt(args=["run", "--threads", "16"])
        assert len(results), 20
