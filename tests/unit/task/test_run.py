import unittest
from argparse import Namespace
from dataclasses import dataclass
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
from dbt.artifacts.resources.v1.model import ModelConfig
from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.config.runtime import RuntimeConfig
from dbt.constants import DEFAULT_HOOK_PRIORITY, PROJECT_HOOK_PRIORITY
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import HookNode, ModelNode
from dbt.events.types import LogModelResult
from dbt.exceptions import DbtRuntimeError
from dbt.flags import get_flags, set_from_args
from dbt.task.run import MicrobatchModelRunner, ModelRunner, RunTask, _get_adapter_info
from dbt.tests.util import safe_set_invocation_context
from dbt_common.events.base_types import EventLevel
from dbt_common.events.event_manager_client import add_callback_to_manager
from tests.utils import EventCatcher


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
            assert isinstance(expected_result, e)


class TestHookPrioritization(unittest.TestCase):
    def test_hook_keyfunc_ordering(self):
        """Test that _hook_keyfunc correctly sorts hooks based on priority"""
        # Create a minimal RunTask instance
        config = mock.MagicMock()
        config.project_name = "test_project"
        task = RunTask(args=mock.MagicMock(), config=config, manifest=mock.MagicMock())

        # Create hook nodes with different priorities
        # High priority package hook
        high_priority_hook = mock.MagicMock()
        high_priority_hook.name = "high_priority_hook"
        high_priority_hook.package_name = "package_a"
        high_priority_hook.config.meta = {"hook_priority": 10}
        high_priority_hook.index = 0

        # Medium priority project hook
        medium_priority_hook = mock.MagicMock()
        medium_priority_hook.name = "medium_priority_hook"
        medium_priority_hook.package_name = "test_project"
        medium_priority_hook.config.meta = {"hook_priority": 30}
        medium_priority_hook.index = 0

        # Default priority package hook
        default_package_hook = mock.MagicMock()
        default_package_hook.name = "default_package_hook"
        default_package_hook.package_name = "package_b"
        default_package_hook.config.meta = {}
        default_package_hook.index = 0

        # Default priority project hook (should be moved to end)
        default_project_hook = mock.MagicMock()
        default_project_hook.name = "default_project_hook"
        default_project_hook.package_name = "test_project"
        default_project_hook.config.meta = {}
        default_project_hook.index = 1

        # Create a list of hooks in a random order
        hooks = [
            default_project_hook,
            high_priority_hook,
            default_package_hook,
            medium_priority_hook,
        ]

        # Sort hooks using _hook_keyfunc
        sorted_hooks = sorted(hooks, key=task._hook_keyfunc)

        # Verify the sorting order
        self.assertEqual(sorted_hooks[0].name, "high_priority_hook")  # Priority 10
        self.assertEqual(sorted_hooks[1].name, "medium_priority_hook")  # Priority 30
        self.assertEqual(sorted_hooks[2].name, "default_package_hook")  # Priority DEFAULT (50)
        self.assertEqual(sorted_hooks[3].name, "default_project_hook")  # Priority PROJECT (100)

    def test_hook_keyfunc_same_priority(self):
        """Test that hooks with same priority are sorted by package name and index"""
        # Create a minimal RunTask instance
        config = mock.MagicMock()
        config.project_name = "test_project"
        task = RunTask(args=mock.MagicMock(), config=config, manifest=mock.MagicMock())

        # Create hook nodes with the same priority
        # Same priority (20), package_b, index 0
        hook1 = mock.MagicMock()
        hook1.name = "hook_b_0"
        hook1.package_name = "package_b"
        hook1.config.meta = {"hook_priority": 20}
        hook1.index = 0

        # Same priority (20), package_a, index 1
        hook2 = mock.MagicMock()
        hook2.name = "hook_a_1"
        hook2.package_name = "package_a"
        hook2.config.meta = {"hook_priority": 20}
        hook2.index = 1

        # Same priority (20), package_a, index 0
        hook3 = mock.MagicMock()
        hook3.name = "hook_a_0"
        hook3.package_name = "package_a"
        hook3.config.meta = {"hook_priority": 20}
        hook3.index = 0

        # Create a list of hooks in a random order
        hooks = [hook1, hook2, hook3]

        # Sort hooks using _hook_keyfunc
        sorted_hooks = sorted(hooks, key=task._hook_keyfunc)

        # Expected order based on package name (alphabetical) and then index:
        # 1. hook_a_0 (package_a, index 0)
        # 2. hook_a_1 (package_a, index 1)
        # 3. hook_b_0 (package_b, index 0)
        self.assertEqual(sorted_hooks[0].name, "hook_a_0")
        self.assertEqual(sorted_hooks[1].name, "hook_a_1")
        self.assertEqual(sorted_hooks[2].name, "hook_b_0")

    def test_project_hooks_default_priority(self):
        """Test that project hooks with default priority are moved to the end"""
        # Create a minimal RunTask instance
        config = mock.MagicMock()
        config.project_name = "test_project"
        task = RunTask(args=mock.MagicMock(), config=config, manifest=mock.MagicMock())

        # Create a package hook with default priority
        package_hook = mock.MagicMock()
        package_hook.name = "package_hook"
        package_hook.package_name = "other_package"
        package_hook.config.meta = {}  # Default priority
        package_hook.index = 0

        # Create a project hook with default priority (should be moved to end)
        project_hook = mock.MagicMock()
        project_hook.name = "project_hook"
        project_hook.package_name = "test_project"
        project_hook.config.meta = {}  # Default priority
        project_hook.index = 0

        # Sort hooks using _hook_keyfunc
        sorted_hooks = sorted([project_hook, package_hook], key=task._hook_keyfunc)

        # Project hook should be after package hook
        self.assertEqual(sorted_hooks[0].name, "package_hook")
        self.assertEqual(sorted_hooks[1].name, "project_hook")

        # Verify the priority values from the key function
        package_key = task._hook_keyfunc(package_hook)
        project_key = task._hook_keyfunc(project_hook)

        # Package hook should have DEFAULT_HOOK_PRIORITY
        self.assertEqual(package_key[0], DEFAULT_HOOK_PRIORITY)

        # Project hook should have PROJECT_HOOK_PRIORITY
        self.assertEqual(project_key[0], PROJECT_HOOK_PRIORITY)
