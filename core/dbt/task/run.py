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

from dbt import tracking
from dbt import utils
from dbt.adapters.base import BaseRelation
from dbt.clients.jinja import MacroGenerator
from dbt.context.providers import generate_runtime_model_context
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.graph.model_config import Hook
from dbt.contracts.graph.parsed import ParsedHookNode
from dbt.contracts.results import NodeStatus, RunResult, RunStatus
from dbt.exceptions import (
    CompilationException,
    InternalException,
    RuntimeException,
    missing_materialization,
)
from dbt.events.functions import fire_event, get_invocation_id
from dbt.events.types import (
    DatabaseErrorRunning, EmptyLine, HooksRunning, HookFinished,
    PrintModelErrorResultLine, PrintModelResultLine, PrintStartLine,
    PrintHookEndLine, PrintHookStartLine
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


class Timer:
    def __init__(self):
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


def _hook_list() -> List[ParsedHookNode]:
    return []


def get_hooks_by_tags(
    nodes: Iterable[CompileResultNode],
    match_tags: Set[str],
) -> List[ParsedHookNode]:
    matched_nodes = []
    for node in nodes:
        if not isinstance(node, ParsedHookNode):
            continue
        node_tags = node.tags
        if len(set(node_tags) & match_tags):
            matched_nodes.append(node)
    return matched_nodes


def get_hook(source, index):
    hook_dict = get_hook_dict(source)
    hook_dict.setdefault('index', index)
    Hook.validate(hook_dict)
    return Hook.from_dict(hook_dict)


def track_model_run(index, num_nodes, run_model_result):
    if tracking.active_user is None:
        raise InternalException('cannot track model run with no active user')
    invocation_id = get_invocation_id()
    tracking.track_model_run({
        "invocation_id": invocation_id,
        "index": index,
        "total": num_nodes,
        "execution_time": run_model_result.execution_time,
        "run_status": str(run_model_result.status).upper(),
        "run_skipped": run_model_result.status == NodeStatus.Skipped,
        "run_error": run_model_result.status == NodeStatus.Error,
        "model_materialization": run_model_result.node.get_materialization(),
        "model_id": utils.get_hash(run_model_result.node),
        "hashed_contents": utils.get_hashed_contents(
            run_model_result.node
        ),
        "timing": [t.to_dict(omit_none=True) for t in run_model_result.timing],
    })


# make sure that we got an ok result back from a materialization
def _validate_materialization_relations_dict(
    inp: Dict[Any, Any], model
) -> List[BaseRelation]:
    try:
        relations_value = inp['relations']
    except KeyError:
        msg = (
            'Invalid return value from materialization, "relations" '
            'not found, got keys: {}'.format(list(inp))
        )
        raise CompilationException(msg, node=model) from None

    if not isinstance(relations_value, list):
        msg = (
            'Invalid return value from materialization, "relations" '
            'not a list, got: {}'.format(relations_value)
        )
        raise CompilationException(msg, node=model) from None

    relations: List[BaseRelation] = []
    for relation in relations_value:
        if not isinstance(relation, BaseRelation):
            msg = (
                'Invalid return value from materialization, '
                '"relations" contains non-Relation: {}'
                .format(relation)
            )
            raise CompilationException(msg, node=model)

        assert isinstance(relation, BaseRelation)
        relations.append(relation)
    return relations


class ModelRunner(CompileRunner):
    def get_node_representation(self):
        display_quote_policy = {
            'database': False, 'schema': False, 'identifier': False
        }
        relation = self.adapter.Relation.create_from(
            self.config, self.node, quote_policy=display_quote_policy
        )
        # exclude the database from output if it's the default
        if self.node.database == self.config.credentials.database:
            relation = relation.include(database=False)
        return str(relation)

    def describe_node(self):
        return "{} model {}".format(self.node.get_materialization(),
                                    self.get_node_representation())

    def print_start_line(self):
        fire_event(
            PrintStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                report_node_data=self.node
            )
        )

    def print_result_line(self, result):
        description = self.describe_node()
        if result.status == NodeStatus.Error:
            fire_event(
                PrintModelErrorResultLine(
                    description=description,
                    status=result.status,
                    index=self.node_index,
                    total=self.num_nodes,
                    execution_time=result.execution_time,
                    report_node_data=self.node
                )
            )
        else:
            fire_event(
                PrintModelResultLine(
                    description=description,
                    status=result.message,
                    index=self.node_index,
                    total=self.num_nodes,
                    execution_time=result.execution_time,
                    report_node_data=self.node
                )
            )

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def _build_run_model_result(self, model, context):
        result = context['load_result']('main')
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
            failures=result.get('failures')
        )

    def _materialization_relations(
        self, result: Any, model
    ) -> List[BaseRelation]:
        if isinstance(result, str):
            msg = (
                'The materialization ("{}") did not explicitly return a '
                'list of relations to add to the cache.'
                .format(str(model.get_materialization()))
            )
            raise CompilationException(msg, node=model)

        if isinstance(result, dict):
            return _validate_materialization_relations_dict(result, model)

        msg = (
            'Invalid return value from materialization, expected a dict '
            'with key "relations", got: {}'.format(str(result))
        )
        raise CompilationException(msg, node=model)

    def execute(self, model, manifest):
        context = generate_runtime_model_context(
            model, self.config, manifest
        )

        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name,
            model.get_materialization(),
            self.adapter.type())

        if materialization_macro is None:
            missing_materialization(model, self.adapter.type())

        if 'config' not in context:
            raise InternalException(
                'Invalid materialization context generated, missing config: {}'
                .format(context)
            )
        context_config = context['config']

        hook_ctx = self.adapter.pre_model_hook(context_config)
        try:
            result = MacroGenerator(materialization_macro, context)()
        finally:
            self.adapter.post_model_hook(context_config, hook_ctx)

        for relation in self._materialization_relations(result, model):
            self.adapter.cache_added(relation.incorporate(dbt_created=True))

        return self._build_run_model_result(model, context)


class RunTask(CompileTask):
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
        statement = compiled.compiled_sql
        hook_index = hook.index or num_hooks
        hook_obj = get_hook(statement, index=hook_index)
        return hook_obj.sql or ''

    def _hook_keyfunc(self, hook: ParsedHookNode) -> Tuple[str, Optional[int]]:
        package_name = hook.package_name
        if package_name == self.config.project_name:
            package_name = BiggestName('')
        return package_name, hook.index

    def get_hooks_by_type(
        self, hook_type: RunHookType
    ) -> List[ParsedHookNode]:

        if self.manifest is None:
            raise InternalException(
                'self.manifest was None in get_hooks_by_type'
            )

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

        startctx = TimestampNamed('node_started_at')
        finishctx = TimestampNamed('node_finished_at')

        for idx, hook in enumerate(ordered_hooks, start=1):
            sql = self.get_hook_sql(adapter, hook, idx, num_hooks,
                                    extra_context)

            hook_text = '{}.{}.{}'.format(hook.package_name, hook_type,
                                          hook.index)
            hook_meta_ctx = HookMetadata(hook, self.index_offset(idx))
            with UniqueID(hook.unique_id):
                with hook_meta_ctx, startctx:
                    fire_event(
                        PrintHookStartLine(
                            statement=hook_text,
                            index=idx,
                            total=num_hooks,
                            truncate=True,
                            report_node_data=hook
                        )
                    )

                status = 'OK'

                with Timer() as timer:
                    if len(sql.strip()) > 0:
                        status, _ = adapter.execute(sql, auto_begin=False,
                                                    fetch=False)
                self.ran_hooks.append(hook)

                with finishctx, DbtModelState({'node_status': 'passed'}):
                    fire_event(
                        PrintHookEndLine(
                            statement=hook_text,
                            status=str(status),
                            index=idx,
                            total=num_hooks,
                            execution_time=timer.elapsed,
                            truncate=True,
                            report_node_data=hook
                        )
                    )

        self._total_executed += len(ordered_hooks)

        with TextOnly():
            fire_event(EmptyLine())

    def safe_run_hooks(
        self, adapter, hook_type: RunHookType, extra_context: Dict[str, Any]
    ) -> None:
        try:
            self.run_hooks(adapter, hook_type, extra_context)
        except RuntimeException:
            fire_event(DatabaseErrorRunning(hook_type))
            raise

    def print_results_line(self, results, execution_time):
        nodes = [r.node for r in results] + self.ran_hooks
        stat_line = get_counts(nodes)

        execution = ""

        if execution_time is not None:
            execution = " in {execution_time:0.2f}s".format(
                execution_time=execution_time)

        with TextOnly():
            fire_event(EmptyLine())
        fire_event(HookFinished(stat_line=stat_line, execution=execution))

    def _get_deferred_manifest(self) -> Optional[WritableManifest]:
        if not self.args.defer:
            return None

        state = self.previous_state
        if state is None:
            raise RuntimeException(
                'Received a --defer argument, but no value was provided '
                'to --state'
            )

        if state.manifest is None:
            raise RuntimeException(
                f'Could not find manifest in --state path: "{self.args.state}"'
            )
        return state.manifest

    def defer_to_manifest(self, adapter, selected_uids: AbstractSet[str]):
        deferred_manifest = self._get_deferred_manifest()
        if deferred_manifest is None:
            return
        if self.manifest is None:
            raise InternalException(
                'Expected to defer to manifest, but there is no runtime '
                'manifest to defer from!'
            )
        self.manifest.merge_from_artifact(
            adapter=adapter,
            other=deferred_manifest,
            selected=selected_uids,
        )
        # TODO: is it wrong to write the manifest here? I think it's right...
        self.write_manifest()

    def before_run(self, adapter, selected_uids: AbstractSet[str]):
        with adapter.connection_named('master'):
            self.create_schemas(adapter, selected_uids)
            self.populate_adapter_cache(adapter)
            self.defer_to_manifest(adapter, selected_uids)
            self.safe_run_hooks(adapter, RunHookType.Start, {})

    def after_run(self, adapter, results):
        # in on-run-end hooks, provide the value 'database_schemas', which is a
        # list of unique (database, schema) pairs that successfully executed
        # models were in. For backwards compatibility, include the old
        # 'schemas', which did not include database information.

        database_schema_set: Set[Tuple[Optional[str], str]] = {
            (r.node.database, r.node.schema) for r in results
            if r.node.is_relational and r.status not in (
                NodeStatus.Error,
                NodeStatus.Fail,
                NodeStatus.Skipped
            )
        }

        self._total_executed += len(results)

        extras = {
            'schemas': list({s for _, s in database_schema_set}),
            'results': results,
            'database_schemas': list(database_schema_set),
        }
        with adapter.connection_named('master'):
            self.safe_run_hooks(adapter, RunHookType.End, extras)

    def after_hooks(self, adapter, results, elapsed):
        self.print_results_line(results, elapsed)

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Model],
        )

    def get_runner_type(self, _):
        return ModelRunner

    def task_end_messages(self, results):
        if results:
            print_run_end_messages(results)
