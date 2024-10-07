import functools
import os
import threading
from copy import deepcopy
from datetime import datetime
from typing import AbstractSet, Any, Dict, Iterable, List, Optional, Set, Tuple, Type

from dbt import tracking, utils
from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.events.types import FinishedRunningStats
from dbt.adapters.exceptions import MissingMaterializationError
from dbt.artifacts.resources import Hook
from dbt.artifacts.schemas.batch_results import BatchResults, BatchType
from dbt.artifacts.schemas.results import (
    NodeStatus,
    RunningStatus,
    RunStatus,
    TimingInfo,
)
from dbt.artifacts.schemas.run import RunResult
from dbt.cli.flags import Flags
from dbt.clients.jinja import MacroGenerator
from dbt.config import RuntimeConfig
from dbt.context.providers import generate_runtime_model_context
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import HookNode, ModelNode, ResultNode
from dbt.events.types import (
    LogHookEndLine,
    LogHookStartLine,
    LogModelResult,
    LogStartLine,
    RunningOperationCaughtError,
)
from dbt.exceptions import CompilationError, DbtInternalError, DbtRuntimeError
from dbt.graph import ResourceTypeSelector
from dbt.hooks import get_hook_dict
from dbt.materializations.incremental.microbatch import MicrobatchBuilder
from dbt.node_types import NodeType, RunHookType
from dbt.task import group_lookup
from dbt.task.base import BaseRunner
from dbt.task.compile import CompileRunner, CompileTask
from dbt.task.printer import get_counts, print_run_end_messages
from dbt_common.clients.jinja import MacroProtocol
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.events.base_types import EventLevel
from dbt_common.events.contextvars import log_contextvars
from dbt_common.events.functions import fire_event, get_invocation_id
from dbt_common.events.types import Formatting
from dbt_common.exceptions import DbtValidationError


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


def get_execution_status(sql: str, adapter: BaseAdapter) -> Tuple[RunStatus, str]:
    if not sql.strip():
        return RunStatus.Success, "OK"

    try:
        response, _ = adapter.execute(sql, auto_begin=False, fetch=False)
        status = RunStatus.Success
        message = response._message
    except DbtRuntimeError as exc:
        status = RunStatus.Error
        message = exc.msg
    finally:
        return status, message


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
        incremental_strategy = node.config.incremental_strategy
    else:
        access = None
        contract_enforced = False
        versioned = False
        incremental_strategy = None
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
            "model_incremental_strategy": incremental_strategy,
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

    def describe_batch(self, batch_start: Optional[datetime]) -> str:
        # Only visualize date if batch_start year/month/day
        formatted_batch_start = MicrobatchBuilder.format_batch_start(
            batch_start, self.node.config.batch_size
        )

        return f"batch {formatted_batch_start} of {self.get_node_representation()}"

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
        group = group_lookup.get(self.node.unique_id)
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
                group=group,
            ),
            level=level,
        )

    def print_batch_result_line(
        self,
        result: RunResult,
        batch_start: Optional[datetime],
        batch_idx: int,
        batch_total: int,
        exception: Optional[Exception],
    ):
        description = self.describe_batch(batch_start)
        group = group_lookup.get(self.node.unique_id)
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
                group=group,
            ),
            level=level,
        )
        if exception:
            fire_event(RunningOperationCaughtError(exc=str(exception)))

    def print_batch_start_line(
        self, batch_start: Optional[datetime], batch_idx: int, batch_total: int
    ) -> None:
        if batch_start is None:
            return

        batch_description = self.describe_batch(batch_start)
        fire_event(
            LogStartLine(
                description=batch_description,
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
            batch_results=None,
        )

    def _build_run_microbatch_model_result(
        self, model: ModelNode, batch_run_results: List[RunResult]
    ) -> RunResult:
        batch_results = BatchResults()
        for result in batch_run_results:
            if result.batch_results is not None:
                batch_results += result.batch_results
            else:
                raise DbtInternalError(
                    "Got a run result without batch results for a batch run, this should be impossible"
                )

        num_successes = len(batch_results.successful)
        num_failures = len(batch_results.failed)

        if num_failures == 0:
            status = RunStatus.Success
            msg = "SUCCESS"
        elif num_successes == 0:
            status = RunStatus.Error
            msg = "ERROR"
        else:
            status = RunStatus.PartialSuccess
            msg = f"PARTIAL SUCCESS ({num_successes}/{num_successes + num_failures})"

        if model.batch_info is not None:
            new_batch_results = deepcopy(model.batch_info)
            new_batch_results.failed = []
            new_batch_results = new_batch_results + batch_results
        else:
            new_batch_results = batch_results

        return RunResult(
            node=model,
            status=status,
            timing=[],
            thread_id=threading.current_thread().name,
            # TODO -- why isn't this getting propagated to logs?
            execution_time=0,
            message=msg,
            adapter_response={},
            failures=num_failures,
            batch_results=new_batch_results,
        )

    def _build_succesful_run_batch_result(
        self, model: ModelNode, context: Dict[str, Any], batch: BatchType
    ) -> RunResult:
        run_result = self._build_run_model_result(model, context)
        run_result.batch_results = BatchResults(successful=[batch])
        return run_result

    def _build_failed_run_batch_result(self, model: ModelNode, batch: BatchType) -> RunResult:
        return RunResult(
            node=model,
            status=RunStatus.Error,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message="ERROR",
            adapter_response={},
            failures=1,
            batch_results=BatchResults(failed=[batch]),
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

    def _execute_model(
        self,
        hook_ctx: Any,
        context_config: Any,
        model: ModelNode,
        context: Dict[str, Any],
        materialization_macro: MacroProtocol,
    ) -> RunResult:
        try:
            result = MacroGenerator(
                materialization_macro, context, stack=context["context_macro_stack"]
            )()
        finally:
            self.adapter.post_model_hook(context_config, hook_ctx)

        for relation in self._materialization_relations(result, model):
            self.adapter.cache_added(relation.incorporate(dbt_created=True))

        return self._build_run_model_result(model, context)

    def _execute_microbatch_model(
        self,
        hook_ctx: Any,
        context_config: Any,
        model: ModelNode,
        manifest: Manifest,
        context: Dict[str, Any],
        materialization_macro: MacroProtocol,
    ) -> RunResult:
        batch_results = None
        try:
            batch_results = self._execute_microbatch_materialization(
                model, manifest, context, materialization_macro
            )
        finally:
            self.adapter.post_model_hook(context_config, hook_ctx)

        if batch_results is not None:
            return self._build_run_microbatch_model_result(model, batch_results)
        else:
            return self._build_run_model_result(model, context)

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

        if (
            os.environ.get("DBT_EXPERIMENTAL_MICROBATCH")
            and model.config.materialized == "incremental"
            and model.config.incremental_strategy == "microbatch"
        ):
            return self._execute_microbatch_model(
                hook_ctx, context_config, model, manifest, context, materialization_macro
            )
        else:
            return self._execute_model(
                hook_ctx, context_config, model, context, materialization_macro
            )

    def _execute_microbatch_materialization(
        self,
        model: ModelNode,
        manifest: Manifest,
        context: Dict[str, Any],
        materialization_macro: MacroProtocol,
    ) -> List[RunResult]:
        batch_results: List[RunResult] = []

        # Note currently (9/30/2024) model.batch_info is only ever _not_ `None`
        # IFF `dbt retry` is being run and the microbatch model had batches which
        # failed on the run of the model (which is being retried)
        if model.batch_info is None:
            microbatch_builder = MicrobatchBuilder(
                model=model,
                is_incremental=self._is_incremental(model),
                event_time_start=getattr(self.config.args, "EVENT_TIME_START", None),
                event_time_end=getattr(self.config.args, "EVENT_TIME_END", None),
            )
            end = microbatch_builder.build_end_time()
            start = microbatch_builder.build_start_time(end)
            batches = microbatch_builder.build_batches(start, end)
        else:
            batches = model.batch_info.failed
            # if there is batch info, then don't run as full_refresh and do force is_incremental
            # not doing this risks blowing away the work that has already been done
            if self._has_relation(model=model):
                context["is_incremental"] = lambda: True
                context["should_full_refresh"] = lambda: False

        # iterate over each batch, calling materialization_macro to get a batch-level run result
        for batch_idx, batch in enumerate(batches):
            self.print_batch_start_line(batch[0], batch_idx + 1, len(batches))

            exception = None
            try:
                # Set start/end in context prior to re-compiling
                model.config["__dbt_internal_microbatch_event_time_start"] = batch[0]
                model.config["__dbt_internal_microbatch_event_time_end"] = batch[1]

                # Recompile node to re-resolve refs with event time filters rendered, update context
                self.compiler.compile_node(
                    model,
                    manifest,
                    {},
                    split_suffix=MicrobatchBuilder.format_batch_start(
                        batch[0], model.config.batch_size
                    ),
                )
                context["model"] = model
                context["sql"] = model.compiled_code
                context["compiled_code"] = model.compiled_code

                # Materialize batch and cache any materialized relations
                result = MacroGenerator(
                    materialization_macro, context, stack=context["context_macro_stack"]
                )()
                for relation in self._materialization_relations(result, model):
                    self.adapter.cache_added(relation.incorporate(dbt_created=True))

                # Build result of executed batch
                batch_run_result = self._build_succesful_run_batch_result(model, context, batch)
                # Update context vars for future batches
                context["is_incremental"] = lambda: True
                context["should_full_refresh"] = lambda: False
            except Exception as e:
                exception = e
                batch_run_result = self._build_failed_run_batch_result(model, batch)

            self.print_batch_result_line(
                batch_run_result, batch[0], batch_idx + 1, len(batches), exception
            )
            batch_results.append(batch_run_result)

        return batch_results

    def _has_relation(self, model) -> bool:
        relation_info = self.adapter.Relation.create_from(self.config, model)
        relation = self.adapter.get_relation(
            relation_info.database, relation_info.schema, relation_info.name
        )
        return relation is not None

    def _is_incremental(self, model) -> bool:
        # TODO: Remove. This is a temporary method. We're working with adapters on
        # a strategy to ensure we can access the `is_incremental` logic without drift
        relation_info = self.adapter.Relation.create_from(self.config, model)
        relation = self.adapter.get_relation(
            relation_info.database, relation_info.schema, relation_info.name
        )
        if (
            relation is not None
            and relation.type == "table"
            and model.config.materialized == "incremental"
        ):
            if model.config.full_refresh is not None:
                return not model.config.full_refresh
            else:
                return not getattr(self.config.args, "FULL_REFRESH", False)
        else:
            return False


class RunTask(CompileTask):
    def __init__(
        self,
        args: Flags,
        config: RuntimeConfig,
        manifest: Manifest,
        batch_map: Optional[Dict[str, BatchResults]] = None,
    ) -> None:
        super().__init__(args, config, manifest)
        self.batch_map = batch_map

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

    def safe_run_hooks(
        self, adapter: BaseAdapter, hook_type: RunHookType, extra_context: Dict[str, Any]
    ) -> RunStatus:
        started_at = datetime.utcnow()
        ordered_hooks = self.get_hooks_by_type(hook_type)

        if hook_type == RunHookType.End and ordered_hooks:
            fire_event(Formatting(""))

        # on-run-* hooks should run outside a transaction. This happens because psycopg2 automatically begins a transaction when a connection is created.
        adapter.clear_transaction()
        if not ordered_hooks:
            return RunStatus.Success

        status = RunStatus.Success
        failed = False
        num_hooks = len(ordered_hooks)

        for idx, hook in enumerate(ordered_hooks, 1):
            with log_contextvars(node_info=hook.node_info):
                hook.index = idx
                hook_name = f"{hook.package_name}.{hook_type}.{hook.index - 1}"
                execution_time = 0.0
                timing = []
                failures = 1

                if not failed:
                    hook.update_event_status(
                        started_at=started_at.isoformat(), node_status=RunningStatus.Started
                    )
                    sql = self.get_hook_sql(adapter, hook, hook.index, num_hooks, extra_context)
                    fire_event(
                        LogHookStartLine(
                            statement=hook_name,
                            index=hook.index,
                            total=num_hooks,
                            node_info=hook.node_info,
                        )
                    )

                    status, message = get_execution_status(sql, adapter)
                    finished_at = datetime.utcnow()
                    hook.update_event_status(finished_at=finished_at.isoformat())
                    execution_time = (finished_at - started_at).total_seconds()
                    timing = [TimingInfo(hook_name, started_at, finished_at)]
                    failures = 0 if status == RunStatus.Success else 1

                    if status == RunStatus.Success:
                        message = f"{hook_name} passed"
                    else:
                        message = f"{hook_name} failed, error:\n {message}"
                        failed = True
                else:
                    status = RunStatus.Skipped
                    message = f"{hook_name} skipped"

                self.node_results.append(
                    RunResult(
                        status=status,
                        thread_id="main",
                        timing=timing,
                        message=message,
                        adapter_response={},
                        execution_time=execution_time,
                        failures=failures,
                        node=hook,
                    )
                )

                fire_event(
                    LogHookEndLine(
                        statement=hook_name,
                        status=status,
                        index=hook.index,
                        total=num_hooks,
                        execution_time=execution_time,
                        node_info=hook.node_info,
                    )
                )

        if hook_type == RunHookType.Start and ordered_hooks:
            fire_event(Formatting(""))

        return status

    def print_results_line(self, results, execution_time) -> None:
        nodes = [r.node for r in results if hasattr(r, "node")]
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

    def populate_microbatch_batches(self, selected_uids: AbstractSet[str]):
        if self.batch_map is not None and self.manifest is not None:
            for uid in selected_uids:
                if uid in self.batch_map:
                    node = self.manifest.ref_lookup.perform_lookup(uid, self.manifest)
                    if isinstance(node, ModelNode):
                        node.batch_info = self.batch_map[uid]

    def before_run(self, adapter: BaseAdapter, selected_uids: AbstractSet[str]) -> RunStatus:
        with adapter.connection_named("master"):
            self.defer_to_manifest()
            required_schemas = self.get_model_schemas(adapter, selected_uids)
            self.create_schemas(adapter, required_schemas)
            self.populate_adapter_cache(adapter, required_schemas)
            self.populate_microbatch_batches(selected_uids)
            group_lookup.init(self.manifest, selected_uids)
            run_hooks_status = self.safe_run_hooks(adapter, RunHookType.Start, {})
            return run_hooks_status

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

    def task_end_messages(self, results) -> None:
        if results:
            print_run_end_messages(results)
