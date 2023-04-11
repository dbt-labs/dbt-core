import os
import time
from pathlib import Path
from abc import abstractmethod
from concurrent.futures import as_completed
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
from typing import Optional, Dict, List, Set, Tuple, Iterable, AbstractSet

from .printer import (
    print_run_result_error,
    print_run_end_messages,
)

from dbt.task.base import ConfiguredTask
from dbt.adapters.base import BaseRelation
from dbt.adapters.factory import get_adapter
from dbt.logger import (
    DbtProcessState,
    TextOnly,
    UniqueID,
    TimestampNamed,
    DbtModelState,
    ModelMetadata,
    NodeCount,
)
from dbt.events.functions import fire_event, warn_or_error
from dbt.events.types import (
    Formatting,
    LogCancelLine,
    DefaultSelector,
    NodeStart,
    NodeFinished,
    QueryCancelationUnsupported,
    ConcurrencyLine,
    EndRunResult,
    NothingToDo,
)
from dbt.events.contextvars import log_contextvars
from dbt.contracts.graph.nodes import SourceDefinition, ResultNode
from dbt.contracts.results import NodeStatus, RunExecutionResult, RunningStatus
from dbt.contracts.state import PreviousState
from dbt.exceptions import (
    DbtInternalError,
    NotImplementedError,
    DbtRuntimeError,
    FailFastError,
)

from dbt.graph import GraphQueue, NodeSelector, SelectionSpec, parse_difference
from dbt.parser.manifest import write_manifest
import dbt.tracking

import dbt.exceptions
from dbt.flags import get_flags
import dbt.utils

RESULT_FILE_NAME = "run_results.json"
RUNNING_STATE = DbtProcessState("running")


class GraphRunnableTask(ConfiguredTask):

    MARK_DEPENDENT_ERRORS_STATUSES = [NodeStatus.Error]

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self.job_queue: Optional[GraphQueue] = None
        self._flattened_nodes: Optional[List[ResultNode]] = None

        self.run_count: int = 0
        self.num_nodes: int = 0
        self.node_results = []
        self._skipped_children = {}
        self._raise_next_tick = None
        self.previous_state: Optional[PreviousState] = None
        self.set_previous_state()

    def set_previous_state(self):
        if self.args.state is not None:
            self.previous_state = PreviousState(
                path=self.args.state, current_path=Path(self.config.target_path)
            )

    def index_offset(self, value: int) -> int:
        return value

    @property
    def selection_arg(self):
        return self.args.select

    @property
    def exclusion_arg(self):
        return self.args.exclude

    def get_selection_spec(self) -> SelectionSpec:
        default_selector_name = self.config.get_default_selector_name()
        # TODO:  The "eager" string below needs to be replaced with programatic access
        #  to the default value for the indirect selection parameter in
        # dbt.cli.params.indirect_selection
        #
        # Doing that is actually a little tricky, so I'm punting it to a new ticket GH #6397
        indirect_selection = getattr(self.args, "INDIRECT_SELECTION", "eager")

        if self.args.selector:
            # use pre-defined selector (--selector)
            spec = self.config.get_selector(self.args.selector)
        elif not (self.selection_arg or self.exclusion_arg) and default_selector_name:
            # use pre-defined selector (--selector) with default: true
            fire_event(DefaultSelector(name=default_selector_name))
            spec = self.config.get_selector(default_selector_name)
        else:
            # use --select and --exclude args
            spec = parse_difference(self.selection_arg, self.exclusion_arg, indirect_selection)
        return spec

    @abstractmethod
    def get_node_selector(self) -> NodeSelector:
        raise NotImplementedError(f"get_node_selector not implemented for task {type(self)}")

    @abstractmethod
    def defer_to_manifest(self, adapter, selected_uids: AbstractSet[str]):
        raise NotImplementedError(f"defer_to_manifest not implemented for task {type(self)}")

    def get_graph_queue(self) -> GraphQueue:
        selector = self.get_node_selector()
        spec = self.get_selection_spec()
        return selector.get_graph_queue(spec)

    def _runtime_initialize(self):
        self.compile_manifest()
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("_runtime_initialize never loaded the graph!")

        self.job_queue = self.get_graph_queue()

        # we use this a couple of times. order does not matter.
        self._flattened_nodes = []
        for uid in self.job_queue.get_selected_nodes():
            if uid in self.manifest.nodes:
                self._flattened_nodes.append(self.manifest.nodes[uid])
            elif uid in self.manifest.sources:
                self._flattened_nodes.append(self.manifest.sources[uid])
            else:
                raise DbtInternalError(
                    f"Node selection returned {uid}, expected a node or a source"
                )

        self.num_nodes = len([n for n in self._flattened_nodes if not n.is_ephemeral_model])

    def raise_on_first_error(self):
        return False

    def get_runner_type(self, node):
        raise NotImplementedError("Not Implemented")

    def result_path(self):
        return os.path.join(self.config.target_path, RESULT_FILE_NAME)

    def get_runner(self, node):
        adapter = get_adapter(self.config)
        run_count: int = 0
        num_nodes: int = 0

        if node.is_ephemeral_model:
            run_count = 0
            num_nodes = 0
        else:
            self.run_count += 1
            run_count = self.run_count
            num_nodes = self.num_nodes

        cls = self.get_runner_type(node)
        return cls(self.config, adapter, node, run_count, num_nodes)

    def call_runner(self, runner):
        uid_context = UniqueID(runner.node.unique_id)
        with RUNNING_STATE, uid_context, log_contextvars(node_info=runner.node.node_info):
            startctx = TimestampNamed("node_started_at")
            index = self.index_offset(runner.node_index)
            runner.node.update_event_status(
                started_at=datetime.utcnow().isoformat(), node_status=RunningStatus.Started
            )
            extended_metadata = ModelMetadata(runner.node, index)

            with startctx, extended_metadata:
                fire_event(
                    NodeStart(
                        node_info=runner.node.node_info,
                    )
                )
            status: Dict[str, str] = {}
            try:
                result = runner.run_with_hooks(self.manifest)
            finally:
                finishctx = TimestampNamed("finished_at")
                with finishctx, DbtModelState(status):
                    fire_event(
                        NodeFinished(
                            node_info=runner.node.node_info,
                            run_result=result.to_msg_dict(),
                        )
                    )
            # `_event_status` dict is only used for logging.  Make sure
            # it gets deleted when we're done with it
            runner.node.clear_event_status()

        fail_fast = get_flags().FAIL_FAST

        if result.status in (NodeStatus.Error, NodeStatus.Fail) and fail_fast:
            self._raise_next_tick = FailFastError(
                msg="Failing early due to test failure or runtime error",
                result=result,
                node=getattr(result, "node", None),
            )
        elif result.status == NodeStatus.Error and self.raise_on_first_error():
            # if we raise inside a thread, it'll just get silently swallowed.
            # stash the error message we want here, and it will check the
            # next 'tick' - should be soon since our thread is about to finish!
            self._raise_next_tick = DbtRuntimeError(result.message)

        return result

    def _submit(self, pool, args, callback):
        """If the caller has passed the magic 'single-threaded' flag, call the
        function directly instead of pool.apply_async. The single-threaded flag
         is intended for gathering more useful performance information about
        what happens beneath `call_runner`, since python's default profiling
        tools ignore child threads.

        This does still go through the callback path for result collection.
        """
        if self.config.args.single_threaded:
            callback(self.call_runner(*args))
        else:
            pool.apply_async(self.call_runner, args=args, callback=callback)

    def _raise_set_error(self):
        if self._raise_next_tick is not None:
            raise self._raise_next_tick

    def run_queue(self, pool):
        """Given a pool, submit jobs from the queue to the pool."""
        if self.job_queue is None:
            raise DbtInternalError("Got to run_queue with no job queue set")

        def callback(result):
            """Note: mark_done, at a minimum, must happen here or dbt will
            deadlock during ephemeral result error handling!
            """
            self._handle_result(result)

            if self.job_queue is None:
                raise DbtInternalError("Got to run_queue callback with no job queue set")
            self.job_queue.mark_done(result.node.unique_id)

        while not self.job_queue.empty():
            node = self.job_queue.get()
            self._raise_set_error()
            runner = self.get_runner(node)
            # we finally know what we're running! Make sure we haven't decided
            # to skip it due to upstream failures
            if runner.node.unique_id in self._skipped_children:
                cause = self._skipped_children.pop(runner.node.unique_id)
                runner.do_skip(cause=cause)
            args = (runner,)
            self._submit(pool, args, callback)

        # block on completion
        if get_flags().FAIL_FAST:
            # checkout for an errors after task completion in case of
            # fast failure
            while self.job_queue.wait_until_something_was_done():
                self._raise_set_error()
        else:
            # wait until every task will be complete
            self.job_queue.join()

        # if an error got set during join(), raise it.
        self._raise_set_error()

        return

    def _handle_result(self, result):
        """Mark the result as completed, insert the `CompileResultNode` into
        the manifest, and mark any descendants (potentially with a 'cause' if
        the result was an ephemeral model) as skipped.
        """
        is_ephemeral = result.node.is_ephemeral_model
        if not is_ephemeral:
            self.node_results.append(result)

        node = result.node

        if self.manifest is None:
            raise DbtInternalError("manifest was None in _handle_result")

        if isinstance(node, SourceDefinition):
            self.manifest.update_source(node)
        else:
            self.manifest.update_node(node)

        if result.status in self.MARK_DEPENDENT_ERRORS_STATUSES:
            if is_ephemeral:
                cause = result
            else:
                cause = None
            self._mark_dependent_errors(node.unique_id, result, cause)

    def _cancel_connections(self, pool):
        """Given a pool, cancel all adapter connections and wait until all
        runners gentle terminates.
        """
        pool.close()
        pool.terminate()

        adapter = get_adapter(self.config)

        if not adapter.is_cancelable():
            fire_event(QueryCancelationUnsupported(type=adapter.type()))
        else:
            with adapter.connection_named("master"):
                for conn_name in adapter.cancel_open_connections():
                    if self.manifest is not None:
                        node = self.manifest.nodes.get(conn_name)
                        if node is not None and node.is_ephemeral_model:
                            continue
                    # if we don't have a manifest/don't have a node, print
                    # anyway.
                    fire_event(LogCancelLine(conn_name=conn_name))

        pool.join()

    def execute_nodes(self):
        num_threads = self.config.threads
        target_name = self.config.target_name

        # following line can be removed when legacy logger is removed
        with NodeCount(self.num_nodes):
            fire_event(
                ConcurrencyLine(
                    num_threads=num_threads, target_name=target_name, node_count=self.num_nodes
                )
            )
        with TextOnly():
            fire_event(Formatting(""))

        pool = ThreadPool(num_threads)
        try:
            self.run_queue(pool)

        except FailFastError as failure:
            self._cancel_connections(pool)
            print_run_result_error(failure.result)
            raise

        except KeyboardInterrupt:
            self._cancel_connections(pool)
            print_run_end_messages(self.node_results, keyboard_interrupt=True)
            raise

        pool.close()
        pool.join()

        return self.node_results

    def _mark_dependent_errors(self, node_id, result, cause):
        if self.graph is None:
            raise DbtInternalError("graph is None in _mark_dependent_errors")
        for dep_node_id in self.graph.get_dependent_nodes(node_id):
            self._skipped_children[dep_node_id] = cause

    def populate_adapter_cache(self, adapter, required_schemas: Set[BaseRelation] = None):
        if not self.args.populate_cache:
            return

        start_populate_cache = time.perf_counter()
        if get_flags().CACHE_SELECTED_ONLY is True:
            adapter.set_relations_cache(self.manifest, required_schemas=required_schemas)
        else:
            adapter.set_relations_cache(self.manifest)
        cache_populate_time = time.perf_counter() - start_populate_cache
        if dbt.tracking.active_user is not None:
            dbt.tracking.track_runnable_timing(
                {"adapter_cache_construction_elapsed": cache_populate_time}
            )

    def before_run(self, adapter, selected_uids: AbstractSet[str]):
        with adapter.connection_named("master"):
            self.populate_adapter_cache(adapter)
            self.defer_to_manifest(adapter, selected_uids)

    def after_run(self, adapter, results):
        pass

    def print_results_line(self, node_results, elapsed):
        pass

    def execute_with_hooks(self, selected_uids: AbstractSet[str]):
        adapter = get_adapter(self.config)
        started = time.time()
        try:
            self.before_run(adapter, selected_uids)
            res = self.execute_nodes()
            self.after_run(adapter, res)
        finally:
            adapter.cleanup_connections()
            elapsed = time.time() - started
            self.print_results_line(self.node_results, elapsed)
            result = self.get_result(
                results=self.node_results, elapsed_time=elapsed, generated_at=datetime.utcnow()
            )

        return result

    def write_result(self, result):
        result.write(self.result_path())

    def run(self):
        """
        Run dbt for the query, based on the graph.
        """
        self._runtime_initialize()

        if self._flattened_nodes is None:
            raise DbtInternalError("after _runtime_initialize, _flattened_nodes was still None")

        if len(self._flattened_nodes) == 0:
            with TextOnly():
                fire_event(Formatting(""))
            warn_or_error(NothingToDo())
            result = self.get_result(
                results=[],
                generated_at=datetime.utcnow(),
                elapsed_time=0.0,
            )
        else:
            with TextOnly():
                fire_event(Formatting(""))
            selected_uids = frozenset(n.unique_id for n in self._flattened_nodes)
            result = self.execute_with_hooks(selected_uids)

        # We have other result types here too, including FreshnessResult
        if isinstance(result, RunExecutionResult):
            result_msgs = [result.to_msg_dict() for result in result.results]
            fire_event(
                EndRunResult(
                    results=result_msgs,
                    generated_at=result.generated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    elapsed_time=result.elapsed_time,
                    success=GraphRunnableTask.interpret_results(result.results),
                )
            )

        if get_flags().WRITE_JSON:
            write_manifest(self.manifest, self.config.target_path)
            self.write_result(result)

        self.task_end_messages(result.results)
        return result

    @classmethod
    def interpret_results(cls, results):
        if results is None:
            return False

        failures = [
            r
            for r in results
            if r.status
            in (
                NodeStatus.RuntimeErr,
                NodeStatus.Error,
                NodeStatus.Fail,
                NodeStatus.Skipped,  # propogate error message causing skip
            )
        ]
        return len(failures) == 0

    def get_model_schemas(self, adapter, selected_uids: Iterable[str]) -> Set[BaseRelation]:
        if self.manifest is None:
            raise DbtInternalError("manifest was None in get_model_schemas")
        result: Set[BaseRelation] = set()

        for node in self.manifest.nodes.values():
            if node.unique_id not in selected_uids:
                continue
            if node.is_relational and not node.is_ephemeral:
                relation = adapter.Relation.create_from(self.config, node)
                result.add(relation.without_identifier())

        return result

    def create_schemas(self, adapter, required_schemas: Set[BaseRelation]):
        # we want the string form of the information schema database
        required_databases: Set[BaseRelation] = set()
        for required in required_schemas:
            db_only = required.include(database=True, schema=False, identifier=False)
            required_databases.add(db_only)

        existing_schemas_lowered: Set[Tuple[Optional[str], Optional[str]]]
        existing_schemas_lowered = set()

        def list_schemas(db_only: BaseRelation) -> List[Tuple[Optional[str], str]]:
            # the database can be None on some warehouses that don't support it
            database_quoted: Optional[str]
            db_lowercase = dbt.utils.lowercase(db_only.database)
            if db_only.database is None:
                database_quoted = None
            else:
                database_quoted = str(db_only)

            # we should never create a null schema, so just filter them out
            return [
                (db_lowercase, s.lower())
                for s in adapter.list_schemas(database_quoted)
                if s is not None
            ]

        def create_schema(relation: BaseRelation) -> None:
            db = relation.database or ""
            schema = relation.schema
            with adapter.connection_named(f"create_{db}_{schema}"):
                adapter.create_schema(relation)

        list_futures = []
        create_futures = []

        with dbt.utils.executor(self.config) as tpe:
            for req in required_databases:
                if req.database is None:
                    name = "list_schemas"
                else:
                    name = f"list_{req.database}"
                fut = tpe.submit_connected(adapter, name, list_schemas, req)
                list_futures.append(fut)

            for ls_future in as_completed(list_futures):
                existing_schemas_lowered.update(ls_future.result())

            for info in required_schemas:
                if info.schema is None:
                    # we are not in the business of creating null schemas, so
                    # skip this
                    continue
                db: Optional[str] = info.database
                db_lower: Optional[str] = dbt.utils.lowercase(db)
                schema: str = info.schema

                db_schema = (db_lower, schema.lower())
                if db_schema not in existing_schemas_lowered:
                    existing_schemas_lowered.add(db_schema)
                    fut = tpe.submit_connected(
                        adapter, f'create_{info.database or ""}_{info.schema}', create_schema, info
                    )
                    create_futures.append(fut)

            for create_future in as_completed(create_futures):
                # trigger/re-raise any excceptions while creating schemas
                create_future.result()

    def get_result(self, results, elapsed_time, generated_at):
        return RunExecutionResult(
            results=results,
            elapsed_time=elapsed_time,
            generated_at=generated_at,
            args=dbt.utils.args_to_dict(self.args),
        )

    def task_end_messages(self, results):
        print_run_end_messages(results)
