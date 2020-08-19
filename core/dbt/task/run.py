import functools
import time
from typing import List, Dict, Any, Iterable, Set, Tuple, Optional, AbstractSet

from .compile import CompileRunner, CompileTask

from .printer import (
    print_start_line,
    print_model_result_line,
    print_hook_start_line,
    print_hook_end_line,
    print_run_end_messages,
    get_counts,
)

from dbt import deprecations
from dbt import tracking
from dbt import utils
from dbt.adapters.base import BaseRelation
from dbt.clients.jinja import MacroGenerator
from dbt.context.providers import generate_runtime_model
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.graph.model_config import Hook
from dbt.contracts.graph.parsed import ParsedHookNode
from dbt.contracts.results import RunModelResult
from dbt.exceptions import (
    CompilationException,
    InternalException,
    RuntimeException,
    missing_materialization,
)
from dbt.logger import (
    GLOBAL_LOGGER as logger,
    TextOnly,
    HookMetadata,
    UniqueID,
    TimestampNamed,
    DbtModelState,
    print_timestamped_line,
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
    return Hook.from_dict(hook_dict)


def track_model_run(index, num_nodes, run_model_result):
    if tracking.active_user is None:
        raise InternalException('cannot track model run with no active user')
    invocation_id = tracking.active_user.invocation_id
    tracking.track_model_run({
        "invocation_id": invocation_id,
        "index": index,
        "total": num_nodes,
        "execution_time": run_model_result.execution_time,
        "run_status": run_model_result.status,
        "run_skipped": run_model_result.skip,
        "run_error": None,
        "model_materialization": run_model_result.node.get_materialization(),
        "model_id": utils.get_hash(run_model_result.node),
        "hashed_contents": utils.get_hashed_contents(
            run_model_result.node
        ),
        "timing": [t.to_dict() for t in run_model_result.timing],
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
        description = self.describe_node()
        print_start_line(description, self.node_index, self.num_nodes)

    def print_result_line(self, result):
        description = self.describe_node()
        print_model_result_line(result, description, self.node_index,
                                self.num_nodes)

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def _build_run_model_result(self, model, context):
        result = context['load_result']('main')
        return RunModelResult(model, status=result.status)

    def _materialization_relations(
        self, result: Any, model
    ) -> List[BaseRelation]:
        if isinstance(result, str):
            deprecations.warn('materialization-return',
                              materialization=model.get_materialization())
            return [
                self.adapter.Relation.create_from(self.config, model)
            ]

        if isinstance(result, dict):
            return _validate_materialization_relations_dict(result, model)

        msg = (
            'Invalid return value from materialization, expected a dict '
            'with key "relations", got: {}'.format(str(result))
        )
        raise CompilationException(msg, node=model)

    def execute(self, model, manifest):
        context = generate_runtime_model(
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
        statement = compiled.injected_sql
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

        plural = 'hook' if num_hooks == 1 else 'hooks'
        with TextOnly():
            print_timestamped_line("")
        print_timestamped_line(
            'Running {} {} {}'.format(num_hooks, hook_type, plural)
        )
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
                    print_hook_start_line(hook_text, idx, num_hooks)

                status = 'OK'

                with Timer() as timer:
                    if len(sql.strip()) > 0:
                        status, _ = adapter.execute(sql, auto_begin=False,
                                                    fetch=False)
                self.ran_hooks.append(hook)

                with finishctx, DbtModelState({'node_status': 'passed'}):
                    print_hook_end_line(
                        hook_text, status, idx, num_hooks, timer.elapsed
                    )

        self._total_executed += len(ordered_hooks)

        with TextOnly():
            print_timestamped_line("")

    def safe_run_hooks(
        self, adapter, hook_type: RunHookType, extra_context: Dict[str, Any]
    ) -> None:
        try:
            self.run_hooks(adapter, hook_type, extra_context)
        except RuntimeException:
            logger.info("Database error while running {}".format(hook_type))
            raise

    def print_results_line(self, results, execution_time):
        nodes = [r.node for r in results] + self.ran_hooks
        stat_line = get_counts(nodes)

        execution = ""

        if execution_time is not None:
            execution = " in {execution_time:0.2f}s".format(
                execution_time=execution_time)

        with TextOnly():
            print_timestamped_line("")
        print_timestamped_line(
            "Finished running {stat_line}{execution}."
            .format(stat_line=stat_line, execution=execution))

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

    def defer_to_manifest(self, selected_uids: AbstractSet[str]):
        deferred_manifest = self._get_deferred_manifest()
        if deferred_manifest is None:
            return
        if self.manifest is None:
            raise InternalException(
                'Expected to defer to manifest, but there is no runtime '
                'manifest to defer from!'
            )
        self.manifest.merge_from_artifact(
            other=deferred_manifest,
            selected=selected_uids,
        )
        # TODO: is it wrong to write the manifest here? I think it's right...
        self.write_manifest()

    def before_run(self, adapter, selected_uids: AbstractSet[str]):
        self.defer_to_manifest(selected_uids)
        with adapter.connection_named('master'):
            self.create_schemas(adapter, selected_uids)
            self.populate_adapter_cache(adapter)
            self.safe_run_hooks(adapter, RunHookType.Start, {})

    def after_run(self, adapter, results):
        # in on-run-end hooks, provide the value 'database_schemas', which is a
        # list  of unique database, schema pairs that successfully executed
        # models  were in. for backwards compatibility, include the old
        # 'schemas', which did not include database information.
        database_schema_set: Set[Tuple[Optional[str], str]] = {
            (r.node.database, r.node.schema) for r in results
            if not any((r.error is not None, r.fail, r.skipped))
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

    def get_runner_type(self):
        return ModelRunner

    def task_end_messages(self, results):
        if results:
            print_run_end_messages(results)
