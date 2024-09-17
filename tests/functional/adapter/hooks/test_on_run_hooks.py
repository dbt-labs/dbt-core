import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import run_dbt_and_capture


class Test__StartHookFail__SelectedNodesSkip__EndHookFail:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "create table {{ target.schema }}.my_start_table ( id int )",  # success
                "drop table {{ target.schema }}.my_start_table",  # success
                "insert into {{ target.schema }}.my_start_table (id) values (1, 2, 3)",  # fail
                "create table {{ target.schema }}.my_start_table ( id int )",  # skip
            ],
            "on-run-end": [
                "create table {{ target.schema }}.my_end_table ( id int )",  # success
                "drop table {{ target.schema }}.my_end_table",  # success
                "insert into {{ target.schema }}.my_end_table (id) values (1, 2, 3)",  # fail
                "create table {{ target.schema }}.my_end_table ( id int )",  # skip
            ],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select * from {{ target.schema }}.my_start_table"
            " union all "
            "select * from {{ target.schema }}.my_end_table"
        }

    def test_results(self, project):
        results, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert [(result.node.alias, result.status) for result in results] == [
            ("test-on-run-start-0", RunStatus.Success),
            ("test-on-run-start-1", RunStatus.Success),
            ("test-on-run-start-2", RunStatus.Error),
            ("test-on-run-start-3", RunStatus.Skipped),
            ("my_model", RunStatus.Skipped),
            ("test-on-run-end-0", RunStatus.Success),
            ("test-on-run-end-1", RunStatus.Success),
            ("test-on-run-end-2", RunStatus.Error),
            ("test-on-run-end-3", RunStatus.Skipped),
        ]

        assert f'relation "{project.test_schema}.my_start_table" does not exist' in log_output
        assert "PASS=4 WARN=0 ERROR=2 SKIP=3 TOTAL=9" in log_output
        assert "8 project hooks, 1 view model" in log_output


class Test__StartHookFail__SelectedNodesSkip__EndHookPass:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "create table {{ target.schema }}.my_start_table ( id int )",  # success
                "drop table {{ target.schema }}.my_start_table",  # success
                "insert into {{ target.schema }}.my_start_table (id) values (1, 2, 3)",  # fail
                "create table {{ target.schema }}.my_start_table ( id int )",  # skip
            ],
            "on-run-end": [
                "create table {{ target.schema }}.my_end_table ( id int )",  # success
                "drop table {{ target.schema }}.my_end_table",  # success
            ],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select * from {{ target.schema }}.my_start_table"
            " union all "
            "select * from {{ target.schema }}.my_end_table"
        }

    def test_results(self, project):
        results, log_output = run_dbt_and_capture(["run"], expect_pass=False)

        assert [(result.node.alias, result.status) for result in results] == [
            ("test-on-run-start-0", RunStatus.Success),
            ("test-on-run-start-1", RunStatus.Success),
            ("test-on-run-start-2", RunStatus.Error),
            ("test-on-run-start-3", RunStatus.Skipped),
            ("my_model", RunStatus.Skipped),
            ("test-on-run-end-0", RunStatus.Success),
            ("test-on-run-end-1", RunStatus.Success),
        ]

        assert f'relation "{project.test_schema}.my_start_table" does not exist' in log_output
        assert "PASS=4 WARN=0 ERROR=1 SKIP=2 TOTAL=7" in log_output
        assert "6 project hooks, 1 view model" in log_output
