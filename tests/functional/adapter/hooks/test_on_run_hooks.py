import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import HookNode
from dbt.tests.util import get_artifact, run_dbt, run_dbt_and_capture
from dbt_common.exceptions import CompilationError


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
    def my_model_run_status(self):
        return RunStatus.Error

    def test_results(self, project, my_model_run_status):
        results, log_output = run_dbt_and_capture(["run"], expect_pass=False)

        expected_results = [
            ("operation.test.test-on-run-start-0", RunStatus.Success),
            ("operation.test.test-on-run-start-1", RunStatus.Success),
            ("operation.test.test-on-run-start-2", RunStatus.Error),
            ("operation.test.test-on-run-start-3", RunStatus.Skipped),
            ("model.test.my_model", my_model_run_status),
        ]

        assert [(result.node.unique_id, result.status) for result in results] == expected_results
        assert [
            (result.node.unique_id, result.node.node_info["node_status"])
            for result in results
            if isinstance(result.node, HookNode)
        ] == [(id, str(status)) for id, status in expected_results if id.startswith("operation")]

        for result in results:
            if result.status == RunStatus.Skipped:
                continue

            timing_keys = [timing.name for timing in result.timing]
            assert timing_keys == ["compile", "execute"]

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


class Test__HookContext__HookSuccess:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "select 1 as id",  # success
                "select 1 as id",  # success
            ],
            "on-run-end": [
                '{{ log("Num Results in context: " ~ results|length)}}'
                "{{ output_thread_ids(results) }}",
            ],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "log.sql": """
{% macro output_thread_ids(results) %}
    {% for result in results %}
        {{ log("Thread ID: " ~ result.thread_id) }}
    {% endfor %}
{% endmacro %}
"""
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1"}

    def test_results_in_context_success(self, project):
        results, log_output = run_dbt_and_capture(["--debug", "run"])
        assert "Thread ID: " in log_output
        assert "Thread ID: main" not in log_output
        assert results[0].thread_id == "main"  # hook still exists in run results
        assert "Num Results in context: 1" in log_output  # only model given hook was successful


class Test__HookContext__HookFail:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "select a as id",  # fail
            ],
            "on-run-end": [
                '{{ log("Num Results in context: " ~ results|length)}}'
                "{{ output_thread_ids(results) }}",
            ],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "log.sql": """
{% macro output_thread_ids(results) %}
    {% for result in results %}
        {{ log("Thread ID: " ~ result.thread_id) }}
    {% endfor %}
{% endmacro %}
"""
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1"}

    def test_results_in_context_hook_fail(self, project):
        results, log_output = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert "Thread ID: main" in log_output
        assert results[0].thread_id == "main"
        assert "Num Results in context: 2" in log_output  # failed hook and model


class Test__HookCompilationError:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "rce.sql": """
{% macro rce(relation) %}
    {% if execute %}
        {{ exceptions.raise_compiler_error("Always raise a compiler error in execute") }}
    {% endif %}
{% endmacro %}
    """
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-end": ["{{ rce() }}"],
        }

    def test_results(self, project):
        with pytest.raises(CompilationError, match="Always raise a compiler error in execute"):
            run_dbt(["run"], expect_pass=False)

        run_results = get_artifact(project.project_root, "target", "run_results.json")
        assert [(result["unique_id"], result["status"]) for result in run_results["results"]] == [
            ("model.test.my_model", RunStatus.Success)
        ]
