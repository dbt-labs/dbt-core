import functools
import os
import threading
import time
from datetime import datetime, timedelta
from typing import AbstractSet, Any, Dict, Iterable, List, Optional, Set, Tuple, Type

import pytz

from dbt import tracking, utils
from dbt.adapters.base import BaseRelation
from dbt.adapters.events.types import (
    DatabaseErrorRunningHook,
    FinishedRunningStats,
    HooksRunning,
)
from dbt.adapters.exceptions import MissingMaterializationError
from dbt.artifacts.resources import Hook, NodeConfig
from dbt.artifacts.resources.types import BatchSize
from dbt.artifacts.schemas.results import (
    BaseResult,
    NodeStatus,
    RunningStatus,
    RunStatus,
)
from dbt.artifacts.schemas.run import RunResult
from dbt.cli.flags import Flags
from dbt.clients.jinja import MacroGenerator
from dbt.config.runtime import RuntimeConfig
from dbt.context.providers import generate_runtime_model_context
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import HookNode, ResultNode
from dbt.events.types import (
    LogHookEndLine,
    LogHookStartLine,
    LogModelResult,
    LogStartLine,
)
from dbt.exceptions import CompilationError, DbtInternalError, DbtRuntimeError
from dbt.graph import ResourceTypeSelector
from dbt.hooks import get_hook_dict
from dbt.node_types import NodeType, RunHookType
from dbt.task.base import BaseRunner
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.events.base_types import EventLevel
from dbt_common.events.contextvars import log_contextvars
from dbt_common.events.functions import fire_event, get_invocation_id
from dbt_common.events.types import Formatting
from dbt_common.exceptions import DbtValidationError

from .compile import CompileRunner, CompileTask
from .printer import get_counts, print_run_end_messages


class Timer:
    def __init__(self) -> None:
        self.start = None
        self.end = None

    @property
    def elapsed(self):
        if self.start is None or self.end is None:
            return None
        return self.end - self.start

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc_value, exc_tracebck):
        self.end = time.time()


@functools.total_ordering
class BiggestName(str):
    def __lt__(self, other):
        return True

    def __eq__(self, other):
        return isinstance(other, self.__class__)


def _hook_list() -> List[HookNode]:
    return []


def get_hooks_by_tags(
    nodes: Iterable[ResultNode],
    match_tags: Set[str],
) -> List[HookNode]:
    matched_nodes = []
    for node in nodes:
        if not isinstance(node, HookNode):
            continue
        node_tags = node.tags
        if len(set(node_tags) & match_tags):
            matched_nodes.append(node)
    return matched_nodes


def get_hook(source, index):
    hook_dict = get_hook_dict(source)
    hook_dict.setdefault("index", index)
    Hook.validate(hook_dict)
    return Hook.from_dict(hook_dict)


def track_model_run(index, num_nodes, run_model_result):
    if tracking.active_user is None:
        raise DbtInternalError("cannot track model run with no active user")
    invocation_id = get_invocation_id()
    node = run_model_result.node
    has_group = True if hasattr(node, "group") and node.group else False
    if node.resource_type == NodeType.Model:
        access = node.access.value if node.access is not None else None
        contract_enforced = node.contract.enforced
        versioned = True if node.version else False
    else:
        access = None
        contract_enforced = False
        versioned = False
    tracking.track_model_run(
        {
            "invocation_id": invocation_id,
            "index": index,
            "total": num_nodes,
            "execution_time": run_model_result.execution_time,
            "run_status": str(run_model_result.status).upper(),
            "run_skipped": run_model_result.status == NodeStatus.Skipped,
            "run_error": run_model_result.status == NodeStatus.Error,
            "model_materialization": node.get_materialization(),
            "model_id": utils.get_hash(node),
            "hashed_contents": utils.get_hashed_contents(node),
            "timing": [t.to_dict(omit_none=True) for t in run_model_result.timing],
            "language": str(node.language),
            "has_group": has_group,
            "contract_enforced": contract_enforced,
            "access": access,
            "versioned": versioned,
        }
    )


# make sure that we got an ok result back from a materialization
def _validate_materialization_relations_dict(inp: Dict[Any, Any], model) -> List[BaseRelation]:
    try:
        relations_value = inp["relations"]
    except KeyError:
        msg = (
            'Invalid return value from materialization, "relations" '
            "not found, got keys: {}".format(list(inp))
        )
        raise CompilationError(msg, node=model) from None

    if not isinstance(relations_value, list):
        msg = (
            'Invalid return value from materialization, "relations" '
            "not a list, got: {}".format(relations_value)
        )
        raise CompilationError(msg, node=model) from None

    relations: List[BaseRelation] = []
    for relation in relations_value:
        if not isinstance(relation, BaseRelation):
            msg = (
                "Invalid return value from materialization, "
                '"relations" contains non-Relation: {}'.format(relation)
            )
            raise CompilationError(msg, node=model)

        assert isinstance(relation, BaseRelation)
        relations.append(relation)
    return relations


class ModelRunner(CompileRunner):
    def get_node_representation(self):
        display_quote_policy = {"database": False, "schema": False, "identifier": False}
        relation = self.adapter.Relation.create_from(
            self.config, self.node, quote_policy=display_quote_policy
        )
        # exclude the database from output if it's the default
        if self.node.database == self.config.credentials.database:
            relation = relation.include(database=False)
        return str(relation)

    def describe_node(self) -> str:
        # TODO CL 'language' will be moved to node level when we change representation
        materialization_strategy = self.node.config.get("incremental_strategy")
        materialization = (
            "microbatch"
            if materialization_strategy == "microbatch"
            else self.node.get_materialization()
        )
        return f"{self.node.language} {materialization} model {self.get_node_representation()}"

    def describe_batch(self, batch_start, batch_end) -> str:
        return f"batch {self.get_node_representation()} from {batch_start} to {batch_end}"

    def print_start_line(self):
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def print_result_line(self, result):
        description = self.describe_node()
        if result.status == NodeStatus.Error:
            status = result.status
            level = EventLevel.ERROR
        else:
            status = result.message
            level = EventLevel.INFO
        fire_event(
            LogModelResult(
                description=description,
                status=status,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                node_info=self.node.node_info,
            ),
            level=level,
        )

    def print_batch_result_line(self, result, description, batch_idx, batch_total):
        if result.status == NodeStatus.Error:
            status = result.status
            level = EventLevel.ERROR
        else:
            status = result.message
            level = EventLevel.INFO
        fire_event(
            LogModelResult(
                description=description,
                status=status,
                index=batch_idx,
                total=batch_total,
                execution_time=result.execution_time,
                node_info=self.node.node_info,
            ),
            level=level,
        )

    def print_batch_start_line(self, description, batch_idx, batch_total):
        fire_event(
            LogStartLine(
                description=description,
                index=batch_idx,
                total=batch_total,
                node_info=self.node.node_info,
            )
        )

    def before_execute(self) -> None:
        self.print_start_line()

    def after_execute(self, result) -> None:
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def _build_run_model_result(self, model, context):
        result = context["load_result"]("main")
        if not result:
            raise DbtRuntimeError("main is not being called during running model")
        adapter_response = {}
        if isinstance(result.response, dbtClassMixin):
            adapter_response = result.response.to_dict(omit_none=True)
        return RunResult(
            node=model,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message=str(result.response),
            adapter_response=adapter_response,
            failures=result.get("failures"),
        )

    def _build_run_microbatch_model_result(self, model, batch_run_results):
        failures = sum([result.failures for result in batch_run_results if result.failures])

        return RunResult(
            node=model,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            # TODO -- why isn't this getting propagated to logs?
            execution_time=None,
            message="SUCCESS",
            adapter_response={},
            failures=failures,
        )

    def _build_failed_run_batch_result(self, model):
        return RunResult(
            node=model,
            status=RunStatus.Error,
            timing=[],
            thread_id=threading.current_thread().name,
            # TODO -- why isn't this getting propagated to logs?
            execution_time=None,
            message="ERROR",
            adapter_response={},
            failures=1,
        )

    def _materialization_relations(self, result: Any, model) -> List[BaseRelation]:
        if isinstance(result, str):
            msg = (
                'The materialization ("{}") did not explicitly return a '
                "list of relations to add to the cache.".format(str(model.get_materialization()))
            )
            raise CompilationError(msg, node=model)

        if isinstance(result, dict):
            return _validate_materialization_relations_dict(result, model)

        msg = (
            "Invalid return value from materialization, expected a dict "
            'with key "relations", got: {}'.format(str(result))
        )
        raise CompilationError(msg, node=model)

    def execute(self, model, manifest):
        context = generate_runtime_model_context(model, self.config, manifest)

        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name, model.get_materialization(), self.adapter.type()
        )

        if materialization_macro is None:
            raise MissingMaterializationError(
                materialization=model.get_materialization(), adapter_type=self.adapter.type()
            )

        if "config" not in context:
            raise DbtInternalError(
                "Invalid materialization context generated, missing config: {}".format(context)
            )
        context_config = context["config"]

        mat_has_supported_langs = hasattr(materialization_macro, "supported_languages")
        model_lang_supported = model.language in materialization_macro.supported_languages
        if mat_has_supported_langs and not model_lang_supported:
            str_langs = [str(lang) for lang in materialization_macro.supported_languages]
            raise DbtValidationError(
                f'Materialization "{materialization_macro.name}" only supports languages {str_langs}; '
                f'got "{model.language}"'
            )

        hook_ctx = self.adapter.pre_model_hook(context_config)
        batch_results = None
        try:
            if (
                os.environ.get("DBT_EXPERIMENTAL_MICROBATCH")
                and model.config.materialized == "incremental"
                and model.config.incremental_strategy == "microbatch"
            ):
                batch_results = self._execute_microbatch_materialization(
                    model, manifest, context, materialization_macro
                )
            else:
                result = MacroGenerator(
                    materialization_macro, context, stack=context["context_macro_stack"]
                )()
                for relation in self._materialization_relations(result, model):
                    self.adapter.cache_added(relation.incorporate(dbt_created=True))
        finally:
            self.adapter.post_model_hook(context_config, hook_ctx)

        if batch_results:
            return self._build_run_microbatch_model_result(model, batch_results)

        return self._build_run_model_result(model, context)

    def _execute_microbatch_materialization(self, model, manifest, context, materialization_macro):
        batch_results = []
        # get the overall start/end bounds
        is_incremental = self._is_incremental(model)
        end: Optional[datetime] = getattr(self.config.args, "EVENT_TIME_END", None)
        end = end.replace(tzinfo=pytz.UTC) if end else self._build_end_time()

        start: Optional[datetime] = getattr(self.config.args, "EVENT_TIME_START", None)
        start = (
            start.replace(tzinfo=pytz.UTC)
            if start
            else self._build_start_time(model, checkpoint=end, is_incremental=is_incremental)
        )
        # split by batch_size
        #   * if full-refresh / first run (is_incremental: False), will need to get a start_time
        #   * option 1: cheap - have user provide start_time (implemented)
        #   * option 2: no start_time, only send one query, no filters (implemented)
        #       * option 2a: min of the min, query each input that has event_time
        if not start:
            batches = [(start, end)]
        else:
            batch_size = model.config.batch_size

            curr_batch_start: datetime = start
            curr_batch_end: datetime = self._offset_timestamp(curr_batch_start, batch_size, 1)

            batches: List[Tuple[datetime, datetime]] = [(curr_batch_start, curr_batch_end)]
            while curr_batch_end <= end:
                curr_batch_start = curr_batch_end
                curr_batch_end = self._offset_timestamp(curr_batch_start, batch_size, 1)
                batches.append((curr_batch_start, curr_batch_end))

            # use exact end value as stop
            batches[-1] = (batches[-1][0], end)

        # iterate over each batch, calling materialization_macro to get a batch-level run result
        for batch_idx, batch in enumerate(batches):
            batch_description = self.describe_batch(batch[0], batch[1])
            model.config["__dbt_internal_microbatch_event_time_start"] = batch[0]
            model.config["__dbt_internal_microbatch_event_time_end"] = batch[1]

            self.print_batch_start_line(batch_description, batch_idx + 1, len(batches))

            exception = None
            try:
                # Recompile node to re-resolve refs with event time filters rendered
                self.compiler.compile_node(model, manifest, {})
                context["model"] = model
                context["sql"] = model.compiled_code
                context["compiled_code"] = model.compiled_code

                result = MacroGenerator(
                    materialization_macro, context, stack=context["context_macro_stack"]
                )()
                for relation in self._materialization_relations(result, model):
                    self.adapter.cache_added(relation.incorporate(dbt_created=True))

                batch_run_result = self._build_run_model_result(model, context)

                context["is_incremental"] = lambda: True
                context["should_full_refresh"] = lambda: False
            except Exception as e:
                exception = e
                batch_run_result = self._build_failed_run_batch_result(model)

            self.print_batch_result_line(
                batch_run_result, batch_description, batch_idx + 1, len(batches)
            )
            if exception:
                print(exception)

            batch_results.append(batch_run_result)

        return batch_results

    def _build_end_time(self) -> Optional[datetime]:
        return datetime.now(tz=pytz.utc)

    def _build_start_time(
        self, model, checkpoint: Optional[datetime], is_incremental: bool
    ) -> Optional[datetime]:
        if not is_incremental or checkpoint is None:
            return None

        assert isinstance(model.config, NodeConfig)
        grain = model.config.batch_size
        if grain is None:
            # TODO: Better error message
            raise DbtRuntimeError("Partition grain not specified")

        lookback = model.config.lookback
        start = self._offset_timestamp(checkpoint, grain, -1 * lookback)

        return start

    def _is_incremental(self, model) -> bool:
        # TODO: Remove. This is a temporary method. We're working with adapters on
        # a strategy to ensure we can access the `is_incremental` logic without drift
        relation_info = self.adapter.Relation.create_from(self.config, model)
        relation = self.adapter.get_relation(
            relation_info.database, relation_info.schema, relation_info.name
        )
        return (
            relation is not None
            and relation.type == "table"
            and model.config.materialized == "incremental"
            and not (getattr(self.config.args, "FULL_REFRESH", False) or model.config.full_refresh)
        )

    def _offset_timestamp(self, timestamp: datetime, grain: BatchSize, offset: int) -> datetime:
        if grain == BatchSize.hour:
            offset_timestamp = datetime(
                timestamp.year,
                timestamp.month,
                timestamp.day,
                timestamp.hour,
                0,
                0,
                0,
                pytz.utc,
            ) + timedelta(hours=offset)
        elif grain == BatchSize.day:
            offset_timestamp = datetime(
                timestamp.year, timestamp.month, timestamp.day, 0, 0, 0, 0, pytz.utc
            ) + timedelta(days=offset)
        elif grain == BatchSize.month:
            offset_timestamp = datetime(timestamp.year, timestamp.month, 1, 0, 0, 0, 0, pytz.utc)
            for _ in range(offset):
                start = timestamp + timedelta(days=1)
                start = datetime(start.year, start.month, 1, 0, 0, 0, 0, pytz.utc)
        elif grain == BatchSize.year:
            offset_timestamp = datetime(timestamp.year + offset, 1, 1, 0, 0, 0, 0, pytz.utc)

        return offset_timestamp


class RunTask(CompileTask):
    def __init__(self, args: Flags, config: RuntimeConfig, manifest: Manifest) -> None:
        super().__init__(args, config, manifest)
        self.ran_hooks: List[HookNode] = []
        self._total_executed = 0

    def index_offset(self, value: int) -> int:
        return self._total_executed + value

    def raise_on_first_error(self) -> bool:
        return False

    def get_hook_sql(self, adapter, hook, idx, num_hooks, extra_context) -> str:
        if self.manifest is None:
            raise DbtInternalError("compile_node called before manifest was loaded")

        compiled = self.compiler.compile_node(hook, self.manifest, extra_context)
        statement = compiled.compiled_code
        hook_index = hook.index or num_hooks
        hook_obj = get_hook(statement, index=hook_index)
        return hook_obj.sql or ""

    def _hook_keyfunc(self, hook: HookNode) -> Tuple[str, Optional[int]]:
        package_name = hook.package_name
        if package_name == self.config.project_name:
            package_name = BiggestName("")
        return package_name, hook.index

    def get_hooks_by_type(self, hook_type: RunHookType) -> List[HookNode]:

        if self.manifest is None:
            raise DbtInternalError("self.manifest was None in get_hooks_by_type")

        nodes = self.manifest.nodes.values()
        # find all hooks defined in the manifest (could be multiple projects)
        hooks: List[HookNode] = get_hooks_by_tags(nodes, {hook_type})
        hooks.sort(key=self._hook_keyfunc)
        return hooks

    def run_hooks(self, adapter, hook_type: RunHookType, extra_context) -> None:
        ordered_hooks = self.get_hooks_by_type(hook_type)

        # on-run-* hooks should run outside of a transaction. This happens
        # b/c psycopg2 automatically begins a transaction when a connection
        # is created.
        adapter.clear_transaction()
        if not ordered_hooks:
            return
        num_hooks = len(ordered_hooks)

        fire_event(Formatting(""))
        fire_event(HooksRunning(num_hooks=num_hooks, hook_type=hook_type))

        for idx, hook in enumerate(ordered_hooks, start=1):
            # We want to include node_info in the appropriate log files, so use
            # log_contextvars
            with log_contextvars(node_info=hook.node_info):
                hook.update_event_status(
                    started_at=datetime.utcnow().isoformat(), node_status=RunningStatus.Started
                )
                sql = self.get_hook_sql(adapter, hook, idx, num_hooks, extra_context)

                hook_text = "{}.{}.{}".format(hook.package_name, hook_type, hook.index)
                fire_event(
                    LogHookStartLine(
                        statement=hook_text,
                        index=idx,
                        total=num_hooks,
                        node_info=hook.node_info,
                    )
                )

                with Timer() as timer:
                    if len(sql.strip()) > 0:
                        response, _ = adapter.execute(sql, auto_begin=False, fetch=False)
                        status = response._message
                    else:
                        status = "OK"

                self.ran_hooks.append(hook)
                hook.update_event_status(finished_at=datetime.utcnow().isoformat())
                hook.update_event_status(node_status=RunStatus.Success)
                fire_event(
                    LogHookEndLine(
                        statement=hook_text,
                        status=status,
                        index=idx,
                        total=num_hooks,
                        execution_time=timer.elapsed,
                        node_info=hook.node_info,
                    )
                )
                # `_event_status` dict is only used for logging.  Make sure
                # it gets deleted when we're done with it
                hook.clear_event_status()

        self._total_executed += len(ordered_hooks)

        fire_event(Formatting(""))

    def safe_run_hooks(
        self, adapter, hook_type: RunHookType, extra_context: Dict[str, Any]
    ) -> None:
        try:
            self.run_hooks(adapter, hook_type, extra_context)
        except DbtRuntimeError as exc:
            fire_event(DatabaseErrorRunningHook(hook_type=hook_type.value))
            self.node_results.append(
                BaseResult(
                    status=RunStatus.Error,
                    thread_id="main",
                    timing=[],
                    message=f"{hook_type.value} failed, error:\n {exc.msg}",
                    adapter_response={},
                    execution_time=0,
                    failures=1,
                )
            )

    def print_results_line(self, results, execution_time) -> None:
        nodes = [r.node for r in results if hasattr(r, "node")] + self.ran_hooks
        stat_line = get_counts(nodes)

        execution = ""

        if execution_time is not None:
            execution = utils.humanize_execution_time(execution_time=execution_time)

        fire_event(Formatting(""))
        fire_event(
            FinishedRunningStats(
                stat_line=stat_line, execution=execution, execution_time=execution_time
            )
        )

    def before_run(self, adapter, selected_uids: AbstractSet[str]) -> None:
        with adapter.connection_named("master"):
            self.defer_to_manifest()
            required_schemas = self.get_model_schemas(adapter, selected_uids)
            self.create_schemas(adapter, required_schemas)
            self.populate_adapter_cache(adapter, required_schemas)
            self.safe_run_hooks(adapter, RunHookType.Start, {})

    def after_run(self, adapter, results) -> None:
        # in on-run-end hooks, provide the value 'database_schemas', which is a
        # list of unique (database, schema) pairs that successfully executed
        # models were in. For backwards compatibility, include the old
        # 'schemas', which did not include database information.

        database_schema_set: Set[Tuple[Optional[str], str]] = {
            (r.node.database, r.node.schema)
            for r in results
            if (hasattr(r, "node") and r.node.is_relational)
            and r.status not in (NodeStatus.Error, NodeStatus.Fail, NodeStatus.Skipped)
        }

        self._total_executed += len(results)

        extras = {
            "schemas": list({s for _, s in database_schema_set}),
            "results": results,
            "database_schemas": list(database_schema_set),
        }
        with adapter.connection_named("master"):
            self.safe_run_hooks(adapter, RunHookType.End, extras)

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Model],
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return ModelRunner

    def get_groups_for_nodes(self, nodes):
        node_to_group_name_map = {i: k for k, v in self.manifest.group_map.items() for i in v}
        group_name_to_group_map = {v.name: v for v in self.manifest.groups.values()}

        return {
            node.unique_id: group_name_to_group_map.get(node_to_group_name_map.get(node.unique_id))
            for node in nodes
        }

    def task_end_messages(self, results) -> None:
        groups = self.get_groups_for_nodes([r.node for r in results if hasattr(r, "node")])

        if results:
            print_run_end_messages(results, groups=groups)
