import os
from unittest import mock

import pytest
from opentelemetry.trace import StatusCode

from dbt.adapters.factory import FACTORY, reset_adapters
from dbt.cli.exceptions import DbtUsageException
from dbt.cli.main import dbtRunner
from dbt.exceptions import DbtProjectError
from dbt.tests.util import read_file, write_file
from dbt.version import __version__ as dbt_version
from dbt_common.events.contextvars import get_node_info
from dbt_common.invocation import get_invocation_id


class TestDbtRunner:
    @pytest.fixture
    def dbt(self) -> dbtRunner:
        return dbtRunner()

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models.sql": "select 1 as id",
        }

    def test_group_invalid_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["--invalid-option"])
        assert type(res.exception) == DbtUsageException

    def test_command_invalid_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["deps", "--invalid-option"])
        assert type(res.exception) == DbtUsageException

    def test_command_mutually_exclusive_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["--warn-error", "--warn-error-options", '{"error": "all"}', "deps"])
        assert type(res.exception) == DbtUsageException
        res = dbt.invoke(["deps", "--warn-error", "--warn-error-options", '{"error": "all"}'])
        assert type(res.exception) == DbtUsageException

        res = dbt.invoke(["compile", "--select", "models", "--inline", "select 1 as id"])
        assert type(res.exception) == DbtUsageException

    def test_invalid_command(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["invalid-command"])
        assert type(res.exception) == DbtUsageException

    def test_invoke_version(self, dbt: dbtRunner) -> None:
        dbt.invoke(["--version"])

    def test_callbacks(self) -> None:
        mock_callback = mock.MagicMock()
        dbt = dbtRunner(callbacks=[mock_callback])
        # the `debug` command is one of the few commands wherein you don't need
        # to have a project to run it and it will emit events
        dbt.invoke(["debug"])
        mock_callback.assert_called()

    def test_callback_node_finished_exceptions_are_raised(self, project):
        from dbt_common.events.base_types import EventMsg

        def callback_with_exception(event: EventMsg):
            if event.info.name == "NodeFinished":
                raise Exception("This should let continue the execution registering the failure")

        dbt = dbtRunner(callbacks=[callback_with_exception])
        result = dbt.invoke(["run", "--select", "models"])

        assert result is not None
        assert (
            result.result.results[0].message
            == "Exception on worker thread. This should let continue the execution registering the failure"
        )

    def test_invoke_kwargs(self, project, dbt):
        res = dbt.invoke(
            ["run"],
            log_format="json",
            log_path="some_random_path",
            version_check=False,
            profile_name="some_random_profile_name",
            target_dir="some_random_target_dir",
        )
        assert res.result.args["log_format"] == "json"
        assert res.result.args["log_path"] == "some_random_path"
        assert res.result.args["version_check"] is False
        assert res.result.args["profile_name"] == "some_random_profile_name"
        assert res.result.args["target_dir"] == "some_random_target_dir"

    def test_invoke_kwargs_project_dir(self, project, dbt):
        res = dbt.invoke(["run"], project_dir="some_random_project_dir")
        assert type(res.exception) == DbtProjectError

        msg = "No dbt_project.yml found at expected path some_random_project_dir"
        assert msg in res.exception.msg

    def test_invoke_kwargs_profiles_dir(self, project, dbt):
        res = dbt.invoke(["run"], profiles_dir="some_random_profiles_dir")
        assert type(res.exception) == DbtProjectError
        msg = "Could not find profile named 'test'"
        assert msg in res.exception.msg

    def test_invoke_kwargs_and_flags(self, project, dbt):
        res = dbt.invoke(["--log-format=text", "run"], log_format="json")
        assert res.result.args["log_format"] == "json"

    def test_pass_in_manifest(self, project, dbt):
        result = dbt.invoke(["parse"])
        manifest = result.result

        reset_adapters()
        assert len(FACTORY.adapters) == 0
        result = dbtRunner(manifest=manifest).invoke(["run"])
        # Check that the adapters are registered again.
        assert result.success
        assert len(FACTORY.adapters) == 1

    def test_pass_in_args_variable(self, dbt):
        args = ["--log-format", "text"]
        args_before = args.copy()
        dbt.invoke(args)
        assert args == args_before

    def test_directory_does_not_change(self, project, dbt: dbtRunner) -> None:
        project_dir = os.getcwd()  # The directory where dbt_project.yml exists.
        os.chdir("../")
        cmd_execution_dir = os.getcwd()  # The directory where dbt command will be run

        commands = ["init", "deps", "clean"]
        for command in commands:
            args = [command, "--project-dir", project_dir]
            if command == "init":
                args.append("--skip-profile-setup")
            res = dbt.invoke(args)
            after_dir = os.getcwd()
            assert res.success is True
            assert cmd_execution_dir == after_dir


class TestDbtRunnerQueryComments:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models.sql": "select 1 as id",
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "query-comment": {
                "comment": f"comment: {dbt_version}",
                "append": True,
            }
        }

    def test_query_comment_saved_manifest(self, project, logs_dir):
        dbt = dbtRunner()
        dbt.invoke(["build", "--select", "models"])
        result = dbt.invoke(["parse"])
        write_file("", logs_dir, "dbt.log")
        # pass in manifest from parse command
        dbt = dbtRunner(result.result)
        dbt.invoke(["build", "--select", "models"])
        log_file = read_file(logs_dir, "dbt.log")
        assert f"comment: {dbt_version}" in log_file


class TestDbtRunnerHooks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models.sql": """
                            {{ config(
                                pre_hook=["select 1"],
                                post_hook="select 2",
                            ) }}
                            select 1 as id
                        """,
            "model2.sql": """
                            {{ config(
                                pre_hook=["select 1", "select 1/0"],
                                post_hook="select 2/0",
                            ) }}
                            select * from {{ ref('models') }}
                        """,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"on-run-end": ["select 1;"]}

    def test_node_info_non_persistence(self, project):
        dbt = dbtRunner()
        dbt.invoke(["run", "--select", "models"])
        assert get_node_info() == {}

    def test_dbt_runner_spans(self, project, otel_spans):
        dbt = dbtRunner()
        dbt.invoke(["--snowflake-projects-otel", "run", "--select", "models", "model2"])
        assert get_node_info() == {}
        exported_spans = otel_spans.get_finished_spans()
        assert exported_spans[0].instrumentation_scope.name == "dbt.runner"
        span_names = [span.name for span in exported_spans]
        span_names.sort()
        # `run_hooks` is called four times per model -- pre and post, each for the
        # inside- and outside-transaction phase -- but filters by `transaction`
        # internally, so only the calls that actually run a hook get a span. Both
        # hooks on `models` run; only model2's pre-hook does, since it fails.
        assert span_names == [
            "dbt.invocation",
            "hooks.post_hook.inside_transaction",
            "hooks.pre_hook.inside_transaction",
            "hooks.pre_hook.inside_transaction",
            "metadata.setup",
            "model.test.model2",
            "model.test.models",
            "on-run-end",
            "operation.test.test-on-run-end-0",
        ]
        model2_span = None
        models_span = None
        metadata_span = None
        invocation_span = None
        for span in exported_spans:
            if span.name == "model.test.model2":
                model2_span = span
            if span.name == "model.test.models":
                models_span = span
            if span.name == "metadata.setup":
                metadata_span = span
            if span.name == "dbt.invocation":
                invocation_span = span

        assert models_span is not None
        assert model2_span is not None
        assert metadata_span is not None
        assert invocation_span is not None

        # The invocation span is the root of the run: it has no parent, and every
        # other span shares its trace and descends from it.
        assert invocation_span.parent is None
        assert invocation_span.attributes["command"] == "run"
        assert invocation_span.attributes["invocation_id"] == get_invocation_id()
        assert invocation_span.attributes["version"] == dbt_version

        for span in exported_spans:
            assert span.context.trace_id == invocation_span.context.trace_id

        # Node and hook spans parent under the invocation span.
        assert models_span.parent.span_id == invocation_span.context.span_id
        assert model2_span.parent.span_id == invocation_span.context.span_id
        assert metadata_span.parent.span_id == invocation_span.context.span_id

        assert "node_outcome" in models_span.attributes
        assert "materialization" in models_span.attributes
        assert "database" in models_span.attributes
        assert "schema" in models_span.attributes
        assert models_span.attributes["node_outcome"] in ("success", "error", "warn", "skipped")
        assert models_span.attributes["materialization"] is not None
        assert models_span.attributes["node_type"] == "model"
        assert "unique_id" in models_span.attributes
        assert "name" in models_span.attributes
        assert "node_type" in models_span.attributes
        assert "identifier" in models_span.attributes
        assert "relative_path" in models_span.attributes

        # Hook spans carry enough context to attribute them to a node and phase.
        hook_spans = [s for s in exported_spans if s.name.startswith("hooks.")]
        assert {s.attributes["unique_id"] for s in hook_spans} == {
            "model.test.models",
            "model.test.model2",
        }
        for hook_span in hook_spans:
            assert hook_span.attributes["inside_transaction"] is True
            assert hook_span.attributes["hook_count"] >= 1

        # Both of `models`' hooks run, in separate phases, and both succeed.
        models_hooks = [s for s in hook_spans if s.attributes["unique_id"] == "model.test.models"]
        assert sorted(s.name for s in models_hooks) == [
            "hooks.post_hook.inside_transaction",
            "hooks.pre_hook.inside_transaction",
        ]
        assert all(s.status.status_code == StatusCode.OK for s in models_hooks)

        # model2's pre-hook raises, so that span records the failure and the
        # post-hook never runs.
        model2_hooks = [s for s in hook_spans if s.attributes["unique_id"] == "model.test.model2"]
        assert [s.name for s in model2_hooks] == ["hooks.pre_hook.inside_transaction"]
        assert model2_hooks[0].attributes["hook_count"] == 2
        assert model2_hooks[0].status.status_code == StatusCode.ERROR

        assert len(model2_span.links) == 1
        assert model2_span.links[0].attributes["upstream.name"] == "model.test.models"
        assert model2_span.links[0].context.span_id == models_span.context.span_id
        assert model2_span.links[0].context.trace_id == models_span.context.trace_id

    def test_dbt_runner_no_spans_when_flag_off(self, project, otel_spans):
        # With the default (--no-snowflake-projects-otel), no spans are emitted.
        dbt = dbtRunner()
        dbt.invoke(["run", "--select", "models", "model2"])
        assert len(otel_spans.get_finished_spans()) == 0


class TestDbtRunnerUnitTestSpans:
    """A model with unit tests is submitted by BuildTask via
    handle_model_with_unit_tests_node rather than _submit, so it needs its own
    coverage that the invocation context still reaches the node spans."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": "select 1 as id",
            "my_model.sql": "select id from {{ ref('model_a') }}",
            "schema.yml": """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('model_a')
        rows:
          - {id: 1}
    expect:
      rows:
        - {id: 1}
""",
        }

    def test_build_with_unit_tests_shares_one_trace(self, project, otel_spans):
        dbt = dbtRunner()
        dbt.invoke(["--snowflake-projects-otel", "build"])

        exported_spans = otel_spans.get_finished_spans()
        by_name = {span.name: span for span in exported_spans}

        invocation_span = by_name["dbt.invocation"]
        model_span = by_name["model.test.my_model"]
        unit_test_span = by_name["unit_test.test.my_model.test_my_model"]

        # Every span belongs to the invocation's trace: no node may start a new one.
        assert {span.context.trace_id for span in exported_spans} == {
            invocation_span.context.trace_id
        }

        # The model and its unit test parent under the invocation span.
        assert model_span.parent.span_id == invocation_span.context.span_id
        assert unit_test_span.parent.span_id == invocation_span.context.span_id
