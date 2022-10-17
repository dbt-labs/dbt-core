import functools
import threading
import time
from typing import List, Dict, Any, Iterable, Set, Tuple, Optional, AbstractSet

from dbt.dataclass_schema import dbtClassMixin

from .compile import CompileRunner, CompileTask

from .printer import (
    print_run_end_messages,
    get_counts,
)
from datetime import datetime
from dbt import tracking
from dbt import utils
from dbt.adapters.base import BaseRelation
from dbt.clients.jinja import MacroGenerator
from dbt.context.providers import generate_runtime_model_context
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.model_config import Hook
from dbt.contracts.graph.parsed import ParsedHookNode
from dbt.contracts.results import NodeStatus, RunResult, RunStatus, RunningStatus
from dbt.exceptions import (
    CompilationException,
    InternalException,
    RuntimeException,
    ValidationException,
    missing_materialization,
)
from dbt.events.functions import fire_event, get_invocation_id
from dbt.events.types import (
    DatabaseErrorRunningHook,
    EmptyLine,
    HooksRunning,
    HookFinished,
    PrintModelErrorResultLine,
    PrintModelResultLine,
    PrintStartLine,
    PrintHookEndLine,
    PrintHookStartLine,
)
from dbt.logger import (
    TextOnly,
    HookMetadata,
    UniqueID,
    TimestampNamed,
    DbtModelState,
)
from dbt.graph import ResourceTypeSelector
from dbt.hooks import get_hook_dict
from dbt.node_types import NodeType, RunHookType


class PreviewTask(CompileTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.ran_hooks = []
        self._total_executed = 0

    def index_offset(self, value: int) -> int:
        return self._total_executed + value

    def raise_on_first_error(self):
        return False

    def get_hook_sql(self, adapter, hook, idx, num_hooks, extra_context):
        compiler = adapter.get_compiler()
        compiled = compiler.compile_node(hook, self.manifest, extra_context)
        statement = compiled.compiled_code
        hook_index = hook.index or num_hooks
        hook_obj = get_hook(statement, index=hook_index)
        return hook_obj.sql or ""

    def _hook_keyfunc(self, hook: ParsedHookNode) -> Tuple[str, Optional[int]]:
        package_name = hook.package_name
        if package_name == self.config.project_name:
            package_name = BiggestName("")
        return package_name, hook.index

    def get_hooks_by_type(self, hook_type: RunHookType) -> List[ParsedHookNode]:

        if self.manifest is None:
            raise InternalException("self.manifest was None in get_hooks_by_type")

        nodes = self.manifest.nodes.values()
        # find all hooks defined in the manifest (could be multiple projects)
        hooks: List[ParsedHookNode] = get_hooks_by_tags(nodes, {hook_type})
        hooks.sort(key=self._hook_keyfunc)
        return hooks

    def run_hooks(self, adapter, hook_type: RunHookType, extra_context):
        ordered_hooks = self.get_hooks_by_type(hook_type)

        # on-run-* hooks should run outside of a transaction. This happens
        # b/c psycopg2 automatically begins a transaction when a connection
        # is created.
        adapter.clear_transaction()
        if not ordered_hooks:
            return
        num_hooks = len(ordered_hooks)

        with TextOnly():
            fire_event(EmptyLine())
        fire_event(HooksRunning(num_hooks=num_hooks, hook_type=hook_type))

        startctx = TimestampNamed("node_started_at")
        finishctx = TimestampNamed("node_finished_at")

        for idx, hook in enumerate(ordered_hooks, start=1):
            hook._event_status["started_at"] = datetime.utcnow().isoformat()
            hook._event_status["node_status"] = RunningStatus.Started
            sql = self.get_hook_sql(adapter, hook, idx, num_hooks, extra_context)

            hook_text = "{}.{}.{}".format(hook.package_name, hook_type, hook.index)
            hook_meta_ctx = HookMetadata(hook, self.index_offset(idx))
            with UniqueID(hook.unique_id):
                with hook_meta_ctx, startctx:
                    fire_event(
                        PrintHookStartLine(
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
                hook._event_status["finished_at"] = datetime.utcnow().isoformat()
                with finishctx, DbtModelState({"node_status": "passed"}):
                    hook._event_status["node_status"] = RunStatus.Success
                    fire_event(
                        PrintHookEndLine(
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
            del hook._event_status["started_at"]
            del hook._event_status["finished_at"]
            del hook._event_status["node_status"]

        self._total_executed += len(ordered_hooks)

        with TextOnly():
            fire_event(EmptyLine())

    def safe_run_hooks(
        self, adapter, hook_type: RunHookType, extra_context: Dict[str, Any]
    ) -> None:
        try:
            self.run_hooks(adapter, hook_type, extra_context)
        except RuntimeException:
            fire_event(DatabaseErrorRunningHook(hook_type=hook_type.value))
            raise

    def print_results_line(self, results, execution_time):
        nodes = [r.node for r in results] + self.ran_hooks
        stat_line = get_counts(nodes)

        execution = ""

        if execution_time is not None:
            execution = utils.humanize_execution_time(execution_time=execution_time)

        with TextOnly():
            fire_event(EmptyLine())
        fire_event(
            HookFinished(stat_line=stat_line, execution=execution, execution_time=execution_time)
        )

    def before_run(self, adapter, selected_uids: AbstractSet[str]):
        with adapter.connection_named("master"):
            required_schemas = self.get_model_schemas(adapter, selected_uids)
            self.create_schemas(adapter, required_schemas)
            self.populate_adapter_cache(adapter, required_schemas)
            self.defer_to_manifest(adapter, selected_uids)
            self.safe_run_hooks(adapter, RunHookType.Start, {})

    def after_run(self, adapter, results):
        # in on-run-end hooks, provide the value 'database_schemas', which is a
        # list of unique (database, schema) pairs that successfully executed
        # models were in. For backwards compatibility, include the old
        # 'schemas', which did not include database information.

        database_schema_set: Set[Tuple[Optional[str], str]] = {
            (r.node.database, r.node.schema)
            for r in results
            if r.node.is_relational
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

    def after_hooks(self, adapter, results, elapsed):
        self.print_results_line(results, elapsed)

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Model],
        )

    def get_runner_type(self, _):
        return SqlExecuteRunner

    def task_end_messages(self, results):
        if results:
            print_run_end_messages(results)
