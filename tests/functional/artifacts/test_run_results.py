import multiprocessing
from pathlib import Path
from time import sleep
import pytest
from dbt.tests.util import run_dbt

good_model_sql = """
select 1 as id
"""

bad_model_sql = """
something bad
"""

slow_model_sql = """
{{ config(materialized='table') }}
select id from {{ ref('good_model') }}, pg_sleep(5)
"""


class TestRunResultsTimingSuccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": good_model_sql}

    def test_timing_exists(self, project):
        results = run_dbt(["run"])
        assert len(results.results) == 1
        assert len(results.results[0].timing) > 0


class TestRunResultsTimingFailure:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": bad_model_sql}

    def test_timing_exists(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results.results) == 1
        assert len(results.results[0].timing) > 0


class TestRunResultsWritesFileOnSignal:
    @pytest.fixture(scope="class")
    def models(self):
        return {"good_model.sql": good_model_sql, "slow_model.sql": slow_model_sql}

    def test_run_results_are_written_on_signal(self, project):

        # N.B. This test is... not great.
        # Due to the way that multiprocessing handles termination this test
        # will always take the entire amount of time designated in pg_sleep.
        # See:
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process.terminate
        #
        # Additionally playing these timing games is probably quite fragile.

        external_process_dbt = multiprocessing.Process(
            target=run_dbt, args=([["run"]]), kwargs={"expect_pass": False}
        )
        external_process_dbt.start()
        assert external_process_dbt.is_alive()
        # More than enough time for the first model to complete
        # but not enough for the second to complete.
        # A bit janky, I know.
        sleep(2)
        external_process_dbt.terminate()
        while external_process_dbt.is_alive() is True:
            pass
        run_results_file = Path(project.project_root) / "target/run_results.json"
        assert run_results_file.is_file()
