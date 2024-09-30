import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import get_artifact, run_dbt_and_capture


class Test__StartHookFail__FlagIsNone__ModelFail:
    @pytest.fixture(scope="class")
    def flags(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(self, flags):
        return {
            "on-run-start": [
                "create table {{ target.schema }}.my_hook_table ( id int )",  # success
                "drop table {{ target.schema }}.my_hook_table",  # success
                "insert into {{ target.schema }}.my_hook_table (id) values (1, 2, 3)",  # fail
                "create table {{ target.schema }}.my_hook_table ( id int )",  # skip
            ],
            "flags": flags,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select * from {{ target.schema }}.my_hook_table"
            " union all "
            "select * from {{ target.schema }}.my_end_table"
        }

    @pytest.fixture(scope="class")
    def log_counts(self):
        return "PASS=2 WARN=0 ERROR=2 SKIP=1 TOTAL=5"

    @pytest.fixture(scope="class")
    def my_model_run_status(self):
        return RunStatus.Error

    def test_results(self, project, log_counts, my_model_run_status):
        results, log_output = run_dbt_and_capture(["run"], expect_pass=False)

        expected_results = [
            ("operation.test.test-on-run-start-0", RunStatus.Success),
            ("operation.test.test-on-run-start-1", RunStatus.Success),
            ("operation.test.test-on-run-start-2", RunStatus.Error),
            ("operation.test.test-on-run-start-3", RunStatus.Skipped),
            ("model.test.my_model", my_model_run_status),
        ]

        assert [(result.node.unique_id, result.status) for result in results] == expected_results
        assert log_counts in log_output
        assert "4 project hooks, 1 view model" in log_output

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        assert [
            (result["unique_id"], result["status"]) for result in run_results["results"]
        ] == expected_results
        assert (
            f'relation "{project.test_schema}.my_hook_table" does not exist'
            in run_results["results"][2]["message"]
        )


class Test__StartHookFail__FlagIsFalse__ModelFail(Test__StartHookFail__FlagIsNone__ModelFail):
    @pytest.fixture(scope="class")
    def flags(self):
        return {"skip_nodes_if_on_run_start_fails": False}


class Test__StartHookFail__FlagIsTrue__ModelSkipped(Test__StartHookFail__FlagIsNone__ModelFail):
    @pytest.fixture(scope="class")
    def flags(self):
        return {"skip_nodes_if_on_run_start_fails": True}

    @pytest.fixture(scope="class")
    def log_counts(self):
        return "PASS=2 WARN=0 ERROR=1 SKIP=2 TOTAL=5"

    @pytest.fixture(scope="class")
    def my_model_run_status(self):
        return RunStatus.Skipped


class Test__ModelPass__EndHookFail:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-end": [
                "create table {{ target.schema }}.my_hook_table ( id int )",  # success
                "drop table {{ target.schema }}.my_hook_table",  # success
                "insert into {{ target.schema }}.my_hook_table (id) values (1, 2, 3)",  # fail
                "create table {{ target.schema }}.my_hook_table ( id int )",  # skip
            ],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1"}

    def test_results(self, project):
        results, log_output = run_dbt_and_capture(["run"], expect_pass=False)

        expected_results = [
            ("model.test.my_model", RunStatus.Success),
            ("operation.test.test-on-run-end-0", RunStatus.Success),
            ("operation.test.test-on-run-end-1", RunStatus.Success),
            ("operation.test.test-on-run-end-2", RunStatus.Error),
            ("operation.test.test-on-run-end-3", RunStatus.Skipped),
        ]

        assert [(result.node.unique_id, result.status) for result in results] == expected_results
        assert "PASS=3 WARN=0 ERROR=1 SKIP=1 TOTAL=5" in log_output
        assert "4 project hooks, 1 view model" in log_output

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        assert [
            (result["unique_id"], result["status"]) for result in run_results["results"]
        ] == expected_results
        assert (
            f'relation "{project.test_schema}.my_hook_table" does not exist'
            in run_results["results"][3]["message"]
        )


class Test__SelectorEmpty__NoHooksRan:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "create table {{ target.schema }}.my_hook_table ( id int )",  # success
                "drop table {{ target.schema }}.my_hook_table",  # success
            ],
            "on-run-end": [
                "create table {{ target.schema }}.my_hook_table ( id int )",  # success
                "drop table {{ target.schema }}.my_hook_table",  # success
            ],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1"}

    def test_results(self, project):
        results, log_output = run_dbt_and_capture(
            ["--debug", "run", "--select", "tag:no_such_tag", "--log-format", "json"]
        )

        assert results.results == []
        assert (
            "The selection criterion 'tag:no_such_tag' does not match any enabled nodes"
            in log_output
        )

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        assert run_results["results"] == []
