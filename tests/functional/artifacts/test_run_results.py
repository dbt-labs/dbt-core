from multiprocessing import Process
from pathlib import Path
import json
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


# This test is failing due to the faulty assumptions that run_results.json would
# be written multiple times. Temporarily disabling.
@pytest.mark.skip()
class TestRunResultsWritesFileOnSignal:
    @pytest.fixture(scope="class")
    def models(self):
        return {"good_model.sql": good_model_sql, "slow_model.sql": slow_model_sql}

    def test_run_results_are_written_on_signal(self, project):
        # Start the runner in a seperate process.
        external_process_dbt = Process(
            target=run_dbt, args=([["run"]]), kwargs={"expect_pass": False}
        )
        external_process_dbt.start()
        assert external_process_dbt.is_alive()

        # Wait until the first file write, then kill the process.
        run_results_file = Path(project.project_root) / "target/run_results.json"
        while run_results_file.is_file() is False:
            pass
        external_process_dbt.terminate()

        # Wait until the process is dead, then check the file that there is only one result.
        while external_process_dbt.is_alive() is True:
            pass
        with run_results_file.open() as run_results_str:
            run_results = json.loads(run_results_str.read())
            assert len(run_results["results"]) == 1
