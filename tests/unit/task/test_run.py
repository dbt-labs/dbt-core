from argparse import Namespace
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from typing import Optional, Type, Union
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from psycopg2 import DatabaseError
from pytest_mock import MockerFixture

from core.dbt.task.run import MicrobatchBatchRunner
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.postgres import PostgresAdapter
from dbt.artifacts.resources.base import FileHash
from dbt.artifacts.resources.types import NodeType, RunHookType
from dbt.artifacts.resources.v1.components import DependsOn
from dbt.artifacts.resources.v1.config import NodeConfig
from dbt.artifacts.resources.v1.model import LatestVersionView, ModelConfig
from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import HookNode, ModelNode
from dbt.events.types import LogModelResult
from dbt.exceptions import DbtRuntimeError
from dbt.flags import get_flags, set_from_args
from dbt.task.run import MicrobatchModelRunner, ModelRunner, RunTask, _get_adapter_info
from dbt.tests.util import safe_set_invocation_context
from dbt_common.events.base_types import EventLevel
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.event_manager_client import add_callback_to_manager


@pytest.mark.parametrize(
    "exception_to_raise, expected_cancel_connections",
    [
        (SystemExit, True),
        (KeyboardInterrupt, True),
        (Exception, False),
    ],
)
def test_run_task_cancel_connections(
    exception_to_raise, expected_cancel_connections, runtime_config: RuntimeConfig
):
    safe_set_invocation_context()

    def mock_run_queue(*args, **kwargs):
        raise exception_to_raise("Test exception")

    with patch.object(RunTask, "run_queue", mock_run_queue), patch.object(
        RunTask, "_cancel_connections"
    ) as mock_cancel_connections:

        set_from_args(Namespace(write_json=False), None)
        task = RunTask(
            get_flags(),
            runtime_config,
            None,
        )
        with pytest.raises(exception_to_raise):
            task.execute_nodes()
        assert mock_cancel_connections.called == expected_cancel_connections


def test_run_task_preserve_edges():
    mock_node_selector = MagicMock()
    mock_spec = MagicMock()
    with patch.object(RunTask, "get_node_selector", return_value=mock_node_selector), patch.object(
        RunTask, "get_selection_spec", return_value=mock_spec
    ):
        task = RunTask(get_flags(), None, None)
        task.get_graph_queue()
        # when we get the graph queue, preserve_edges is True
        mock_node_selector.get_graph_queue.assert_called_with(mock_spec, True)


def test_tracking_fails_safely_for_missing_adapter():
    assert {} == _get_adapter_info(None, {})


def test_adapter_info_tracking():
    mock_run_result = MagicMock()
    mock_run_result.node = MagicMock()
    mock_run_result.node.config = {}
    assert _get_adapter_info(PostgresAdapter, mock_run_result) == {
        "model_adapter_details": {},
        "adapter_name": PostgresAdapter.__name__.split("Adapter")[0].lower(),
        "adapter_version": import_module("dbt.adapters.postgres.__version__").version,
        "base_adapter_version": import_module("dbt.adapters.__about__").version,
    }


class TestModelRunner:
    @pytest.fixture
    def log_model_result_catcher(self) -> EventCatcher:
        catcher = EventCatcher(event_to_catch=LogModelResult)
        add_callback_to_manager(catcher.catch)
        return catcher

    @pytest.fixture
    def model_runner(
        self,
        postgres_adapter: PostgresAdapter,
        table_model: ModelNode,
        runtime_config: RuntimeConfig,
    ) -> ModelRunner:
        return ModelRunner(
            config=runtime_config,
            adapter=postgres_adapter,
            node=table_model,
            node_index=1,
            num_nodes=1,
        )

    @pytest.fixture
    def run_result(self, table_model: ModelNode) -> RunResult:
        return RunResult(
            status=RunStatus.Success,
            timing=[],
            thread_id="an_id",
            execution_time=0,
            adapter_response={},
            message="It did it",
            failures=None,
            batch_results=None,
            node=table_model,
        )

    def test_print_result_line(
        self,
        log_model_result_catcher: EventCatcher,
        model_runner: ModelRunner,
        run_result: RunResult,
    ) -> None:
        # Check `print_result_line` with "successful" RunResult
        model_runner.print_result_line(run_result)
        assert len(log_model_result_catcher.caught_events) == 1
        assert log_model_result_catcher.caught_events[0].info.level == EventLevel.INFO
        assert log_model_result_catcher.caught_events[0].data.status == run_result.message

        # reset event catcher
        log_model_result_catcher.flush()

        # Check `print_result_line` with "error" RunResult
        run_result.status = RunStatus.Error
        model_runner.print_result_line(run_result)
        assert len(log_model_result_catcher.caught_events) == 1
        assert log_model_result_catcher.caught_events[0].info.level == EventLevel.ERROR
        assert log_model_result_catcher.caught_events[0].data.status == EventLevel.ERROR

    @pytest.mark.skip(
        reason="Default and adapter macros aren't being appropriately populated, leading to a runtime error"
    )
    def test_execute(
        self, table_model: ModelNode, manifest: Manifest, model_runner: ModelRunner
    ) -> None:
        model_runner.execute(model=table_model, manifest=manifest)
        # TODO: Assert that the model was executed

    def test_materialize_latest_version_view_for_latest_version(
        self, mocker: MockerFixture, model_runner: ModelRunner
    ) -> None:
        @dataclass
        class FakeRelation:
            database: str
            schema: str
            identifier: str
            type: str

            @property
            def name(self) -> str:
                return self.identifier

            def __str__(self) -> str:
                return f'"{self.database}"."{self.schema}"."{self.identifier}"'

        model = model_runner.node
        model.name = "versioned_model"
        model.version = 2
        model.latest_version = 2
        model.config = ModelConfig(
            materialized="table",
            latest_version_view=LatestVersionView(enabled=True),
        )

        source_relation = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="versioned_model_v2", type="table"
        )
        pointer_relation = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="versioned_model", type="view"
        )

        model_runner.adapter = mocker.Mock()
        model_runner.adapter.Relation.create.return_value = pointer_relation
        model_runner.adapter.get_relation.return_value = None

        manifest = mocker.Mock(spec=Manifest)
        # Return None for alias macro (fall back to model.name), sentinel for DDL macros
        manifest.find_macro_by_name.side_effect = lambda name, *_: (
            None if name == "generate_latest_version_view_alias" else mocker.sentinel.pointer_macro
        )

        macro_generator = mocker.Mock(return_value="create latest pointer sql")
        mocker.patch("dbt.task.run.MacroGenerator", return_value=macro_generator)

        pointer_relations = model_runner._materialize_latest_version_view(
            manifest=manifest,
            model=model,
            context={"context_macro_stack": []},
            relations=[source_relation],
        )

        assert pointer_relations == [pointer_relation]
        model_runner.adapter.Relation.create.assert_called_once_with(
            database="dbt",
            schema="dbt_schema",
            identifier="versioned_model",
            type="view",
        )
        manifest.find_macro_by_name.assert_any_call(
            "get_create_sql", model_runner.config.project_name, None
        )
        macro_generator.assert_called_once_with(
            pointer_relation, 'select * from "dbt"."dbt_schema"."versioned_model_v2"'
        )
        model_runner.adapter.execute.assert_called_once_with(
            "create latest pointer sql", auto_begin=False, fetch=False
        )

    def test_materialize_latest_version_view_uses_custom_alias(
        self, mocker: MockerFixture, model_runner: ModelRunner
    ) -> None:
        @dataclass
        class FakeRelation:
            database: str
            schema: str
            identifier: str
            type: str

            @property
            def name(self) -> str:
                return self.identifier

        model = model_runner.node
        model.name = "versioned_model"
        model.version = 2
        model.latest_version = 2
        model.config = ModelConfig(
            materialized="table",
            latest_version_view=LatestVersionView(enabled=True, alias="latest_alias"),
        )

        source_relation = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="versioned_model_v2", type="table"
        )
        pointer_relation = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="latest_alias", type="view"
        )

        model_runner.adapter = mocker.Mock()
        model_runner.adapter.Relation.create.return_value = pointer_relation
        model_runner.adapter.get_relation.return_value = None

        manifest = mocker.Mock(spec=Manifest)
        manifest.find_macro_by_name.side_effect = lambda name, *_: (
            None if name == "generate_latest_version_view_alias" else mocker.sentinel.pointer_macro
        )

        mocker.patch("dbt.task.run.MacroGenerator", return_value=mocker.Mock(return_value="sql"))

        model_runner._materialize_latest_version_view(
            manifest=manifest,
            model=model,
            context={"context_macro_stack": []},
            relations=[source_relation],
        )

        model_runner.adapter.Relation.create.assert_called_once_with(
            database="dbt",
            schema="dbt_schema",
            identifier="latest_alias",
            type="view",
        )

    @pytest.mark.parametrize(
        "version,latest_version,latest_version_view_enabled",
        [
            (1, 2, True),
            (2, 2, False),
        ],
    )
    def test_materialize_latest_version_view_skips_when_not_needed(
        self,
        mocker: MockerFixture,
        model_runner: ModelRunner,
        version: int,
        latest_version: int,
        latest_version_view_enabled: bool,
    ) -> None:
        @dataclass
        class FakeRelation:
            database: str
            schema: str
            identifier: str
            type: str

            @property
            def name(self) -> str:
                return self.identifier

        model = model_runner.node
        model.name = "versioned_model"
        model.version = version
        model.latest_version = latest_version
        model.config = ModelConfig(
            materialized="table",
            latest_version_view=LatestVersionView(enabled=latest_version_view_enabled),
        )

        model_runner.adapter = mocker.Mock()
        manifest = mocker.Mock(spec=Manifest)

        pointer_relations = model_runner._materialize_latest_version_view(
            manifest=manifest,
            model=model,
            context={"context_macro_stack": []},
            relations=[
                FakeRelation(
                    database="dbt",
                    schema="dbt_schema",
                    identifier="versioned_model_v2",
                    type="table",
                )
            ],
        )

        assert pointer_relations == []
        model_runner.adapter.Relation.create.assert_not_called()
        manifest.find_macro_by_name.assert_not_called()

    def test_materialize_latest_version_view_drops_and_recreates_existing_relation(
        self, mocker: MockerFixture, model_runner: ModelRunner
    ) -> None:
        @dataclass
        class FakeRelation:
            database: str
            schema: str
            identifier: str
            type: str

            @property
            def name(self) -> str:
                return self.identifier

            def __str__(self) -> str:
                return f'"{self.database}"."{self.schema}"."{self.identifier}"'

        model = model_runner.node
        model.name = "versioned_model"
        model.version = 2
        model.latest_version = 2
        model.config = ModelConfig(
            materialized="table",
            latest_version_view=LatestVersionView(enabled=True),
        )

        source_relation = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="versioned_model_v2", type="table"
        )
        pointer_relation = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="versioned_model", type="view"
        )
        existing_table = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="versioned_model", type="table"
        )

        model_runner.adapter = mocker.Mock()
        model_runner.adapter.Relation.create.return_value = pointer_relation
        model_runner.adapter.get_relation.return_value = existing_table

        manifest = mocker.Mock(spec=Manifest)
        manifest.find_macro_by_name.side_effect = lambda name, *_: (
            None if name == "generate_latest_version_view_alias" else mocker.sentinel.pointer_macro
        )

        macro_generator = mocker.Mock(return_value="create view sql")
        mocker.patch("dbt.task.run.MacroGenerator", return_value=macro_generator)

        pointer_relations = model_runner._materialize_latest_version_view(
            manifest=manifest,
            model=model,
            context={"context_macro_stack": []},
            relations=[source_relation],
        )

        assert pointer_relations == [pointer_relation]
        model_runner.adapter.drop_relation.assert_called_once_with(existing_table)
        model_runner.adapter.execute.assert_called_once_with(
            "create view sql", auto_begin=False, fetch=False
        )

    def test_materialize_latest_version_view_errors_on_alias_collision(
        self, mocker: MockerFixture, model_runner: ModelRunner
    ) -> None:
        @dataclass
        class FakeRelation:
            database: str
            schema: str
            identifier: str
            type: str

            @property
            def name(self) -> str:
                return self.identifier

        model = model_runner.node
        model.name = "versioned_model"
        model.version = 2
        model.latest_version = 2
        model.config = ModelConfig(
            materialized="table",
            latest_version_view=LatestVersionView(enabled=True, alias="versioned_model_v2"),
        )

        source_relation = FakeRelation(
            database="dbt", schema="dbt_schema", identifier="versioned_model_v2", type="table"
        )

        model_runner.adapter = mocker.Mock()
        manifest = mocker.Mock(spec=Manifest)

        with pytest.raises(DbtRuntimeError, match="already aliased"):
            model_runner._materialize_latest_version_view(
                manifest=manifest,
                model=model,
                context={"context_macro_stack": []},
                relations=[source_relation],
            )


class TestMicrobatchModelRunner:
    @pytest.fixture
    def model_runner(
        self,
        postgres_adapter: PostgresAdapter,
        table_model: ModelNode,
        runtime_config: RuntimeConfig,
    ) -> MicrobatchModelRunner:
        return MicrobatchModelRunner(
            config=runtime_config,
            adapter=postgres_adapter,
            node=table_model,
            node_index=1,
            num_nodes=1,
        )

    @pytest.fixture
    def batch_runner(
        self,
        postgres_adapter: PostgresAdapter,
        table_model: ModelNode,
        runtime_config: RuntimeConfig,
    ) -> MicrobatchBatchRunner:
        return MicrobatchBatchRunner(
            config=runtime_config,
            adapter=postgres_adapter,
            node=table_model,
            node_index=1,
            num_nodes=1,
            batch_idx=0,
            batches=[],
            relation_exists=False,
            incremental_batch=False,
        )

    @pytest.mark.parametrize(
        "has_relation,relation_type,materialized,full_refresh_config,full_refresh_flag,expectation",
        [
            (False, "table", "incremental", None, False, False),
            (True, "other", "incremental", None, False, False),
            (True, "table", "other", None, False, False),
            # model config takes precendence
            (True, "table", "incremental", True, False, False),
            # model config takes precendence
            (True, "table", "incremental", True, True, False),
            # model config takes precendence
            (True, "table", "incremental", False, False, True),
            # model config takes precendence
            (True, "table", "incremental", False, True, True),
            # model config is none, so opposite flag value
            (True, "table", "incremental", None, True, False),
            # model config is none, so opposite flag value
            (True, "table", "incremental", None, False, True),
        ],
    )
    def test__is_incremental(
        self,
        mocker: MockerFixture,
        model_runner: MicrobatchModelRunner,
        has_relation: bool,
        relation_type: str,
        materialized: str,
        full_refresh_config: Optional[bool],
        full_refresh_flag: bool,
        expectation: bool,
    ) -> None:

        # Setup adapter relation getting
        @dataclass
        class RelationInfo:
            database: str = "database"
            schema: str = "schema"
            name: str = "name"

        @dataclass
        class Relation:
            type: str

        model_runner.adapter = mocker.Mock()
        model_runner.adapter.Relation.create_from.return_value = RelationInfo()

        if has_relation:
            model_runner.adapter.get_relation.return_value = Relation(type=relation_type)
        else:
            model_runner.adapter.get_relation.return_value = None

        # Set ModelRunner configs
        model_runner.config.args = Namespace(FULL_REFRESH=full_refresh_flag)

        # Create model with configs
        model = model_runner.node
        model.config = ModelConfig(materialized=materialized, full_refresh=full_refresh_config)

        # Assert result of _is_incremental
        assert model_runner._is_incremental(model) == expectation

    @pytest.mark.parametrize(
        "adapter_microbatch_concurrency,has_relation,concurrent_batches,has_this,expectation",
        [
            (True, True, None, False, True),
            (True, True, None, True, False),
            (True, True, True, False, True),
            (True, True, True, True, True),
            (True, True, False, False, False),
            (True, True, False, True, False),
            (True, False, None, False, False),
            (True, False, None, True, False),
            (True, False, True, False, False),
            (True, False, True, True, False),
            (True, False, False, False, False),
            (True, False, False, True, False),
            (False, True, None, False, False),
            (False, True, None, True, False),
            (False, True, True, False, False),
            (False, True, True, True, False),
            (False, True, False, False, False),
            (False, True, False, True, False),
            (False, False, None, False, False),
            (False, False, None, True, False),
            (False, False, True, False, False),
            (False, False, True, True, False),
            (False, False, False, False, False),
            (False, False, False, True, False),
        ],
    )
    def test_should_run_in_parallel(
        self,
        mocker: MockerFixture,
        batch_runner: MicrobatchBatchRunner,
        adapter_microbatch_concurrency: bool,
        has_relation: bool,
        concurrent_batches: Optional[bool],
        has_this: bool,
        expectation: bool,
    ) -> None:
        batch_runner.node._has_this = has_this
        batch_runner.node.config = ModelConfig(concurrent_batches=concurrent_batches)
        batch_runner.relation_exists = has_relation

        mocked_supports = mocker.patch.object(batch_runner.adapter, "supports")
        mocked_supports.return_value = adapter_microbatch_concurrency

        # Assert result of should_run_in_parallel
        assert batch_runner.should_run_in_parallel() == expectation

    def test_get_microbatch_builder_uses_original_invocation_time_on_retry(
        self,
        mocker: MockerFixture,
        model_runner: MicrobatchModelRunner,
    ) -> None:
        """When retrying, get_microbatch_builder should use the original invocation
        time from the previous run rather than the current invocation time."""
        original_time = datetime(2025, 3, 21, 7, 55, 0)
        current_time = datetime(2025, 3, 25, 1, 4, 0)

        # Set up a mock parent task with original_invocation_started_at
        mock_parent = mocker.Mock(spec=RunTask)
        mock_parent.original_invocation_started_at = original_time
        model_runner._parent_task = mock_parent

        # Mock _is_incremental to avoid adapter calls
        mocker.patch.object(model_runner, "_is_incremental", return_value=True)

        # Mock get_invocation_started_at to return the "current" (retry) time
        mocker.patch(
            "dbt.task.run.get_invocation_started_at",
            return_value=current_time,
        )

        model = model_runner.node
        model.config.materialized = "incremental"
        model.config.incremental_strategy = "microbatch"
        model.config.batch_size = "day"
        model.config.begin = "2024-12-01"
        model.config.event_time = "_event_date"
        model_runner.config.args = Namespace(
            EVENT_TIME_START=None, EVENT_TIME_END=None, SAMPLE=None
        )

        builder = model_runner.get_microbatch_builder(model)
        assert builder.default_end_time == original_time

    def test_get_microbatch_builder_uses_current_time_without_retry(
        self,
        mocker: MockerFixture,
        model_runner: MicrobatchModelRunner,
    ) -> None:
        """When not retrying (normal run), get_microbatch_builder should use
        the current invocation time."""
        current_time = datetime(2025, 3, 25, 1, 4, 0)

        # Set up a mock parent task with no original_invocation_started_at
        mock_parent = mocker.Mock(spec=RunTask)
        mock_parent.original_invocation_started_at = None
        model_runner._parent_task = mock_parent

        # Mock _is_incremental to avoid adapter calls
        mocker.patch.object(model_runner, "_is_incremental", return_value=True)

        mocker.patch(
            "dbt.task.run.get_invocation_started_at",
            return_value=current_time,
        )

        model = model_runner.node
        model.config.materialized = "incremental"
        model.config.incremental_strategy = "microbatch"
        model.config.batch_size = "day"
        model.config.begin = "2024-12-01"
        model.config.event_time = "_event_date"
        model_runner.config.args = Namespace(
            EVENT_TIME_START=None, EVENT_TIME_END=None, SAMPLE=None
        )

        builder = model_runner.get_microbatch_builder(model)
        assert builder.default_end_time == current_time


class TestRunTask:
    @pytest.fixture
    def hook_node(self) -> HookNode:
        return HookNode(
            package_name="test",
            path="/root/x/path.sql",
            original_file_path="/root/path.sql",
            language="sql",
            raw_code="select * from wherever",
            name="foo",
            resource_type=NodeType.Operation,
            unique_id="model.test.foo",
            fqn=["test", "models", "foo"],
            refs=[],
            sources=[],
            metrics=[],
            depends_on=DependsOn(),
            description="",
            database="test_db",
            schema="test_schema",
            alias="bar",
            tags=[],
            config=NodeConfig(),
            index=None,
            checksum=FileHash.from_contents(""),
            unrendered_config={},
        )

    @pytest.mark.parametrize(
        "error_to_raise,expected_result",
        [
            (None, RunStatus.Success),
            (DbtRuntimeError, RunStatus.Error),
            (DatabaseError, RunStatus.Error),
            (KeyboardInterrupt, KeyboardInterrupt),
        ],
    )
    def test_safe_run_hooks(
        self,
        mocker: MockerFixture,
        runtime_config: RuntimeConfig,
        manifest: Manifest,
        hook_node: HookNode,
        error_to_raise: Optional[Type[Exception]],
        expected_result: Union[RunStatus, Type[Exception]],
    ):
        mocker.patch("dbt.task.run.RunTask.get_hooks_by_type").return_value = [hook_node]
        mocker.patch("dbt.task.run.RunTask.get_hook_sql").return_value = hook_node.raw_code

        flags = mock.Mock()
        flags.state = None
        flags.defer_state = None

        run_task = RunTask(
            args=flags,
            config=runtime_config,
            manifest=manifest,
        )

        adapter = mock.Mock()
        adapter_execute = mock.Mock()
        adapter_execute.return_value = (AdapterResponse(_message="Success"), None)

        if error_to_raise:
            adapter_execute.side_effect = error_to_raise("Oh no!")

        adapter.execute = adapter_execute

        try:
            result = run_task.safe_run_hooks(
                adapter=adapter,
                hook_type=RunHookType.End,
                extra_context={},
            )
            assert isinstance(expected_result, RunStatus)
            assert result == expected_result
        except BaseException as e:
            assert not isinstance(expected_result, RunStatus)
            assert issubclass(expected_result, BaseException)
            assert type(e) == expected_result
