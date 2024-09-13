import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import run_dbt_and_capture


class Test__StartHookFail__SelectedNodesSkip__EndHookFail:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "create table if not exists {{ target.schema }}.my_start_table ( id int )",  # success
                "drop table if exists {{ target.schema }}.my_start_table",  # success
                "insert into {{ target.schema }}.my_start_table (id) values (1, 2, 3)",  # fail
                "create table if not exists {{ target.schema }}.my_start_table ( id int )",  # skip
            ],
            "on-run-end": [
                "create table if not exists {{ target.schema }}.my_end_table ( id int )",  # success
                "drop table if exists {{ target.schema }}.my_end_table",  # success
                "insert into {{ target.schema }}.my_end_table (id) values (1, 2, 3)",  # fail
                "create table if not exists {{ target.schema }}.my_end_table ( id int )",  # skip
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
        assert [result.status for result in results] == [
            RunStatus.Error,
            RunStatus.Skipped,
            RunStatus.Error,
        ]


class StartHookFail__SelectedNodesSkip__EndHookPass:
    pass
