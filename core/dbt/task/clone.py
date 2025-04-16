import threading
from typing import AbstractSet, Any, Collection, Iterable, List, Optional, Set, Type

from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.schemas.run import RunResult, RunStatus
from dbt.clients.jinja import MacroGenerator
from dbt.context.providers import generate_runtime_model_context
from dbt.contracts.graph.manifest import Manifest
from dbt.graph import ResourceTypeSelector
from dbt.node_types import REFABLE_NODE_TYPES
from dbt.task.base import BaseRunner, resource_types_from_args
from dbt.task.run import _validate_materialization_relations_dict
from dbt.task.runnable import GraphRunnableMode, GraphRunnableTask
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import CompilationError, DbtInternalError


class CloneRunner(BaseRunner):
    def before_execute(self) -> None:
        pass

    def after_execute(self, result) -> None:
        pass

    def _build_run_model_result(self, model, context):
        result = context["load_result"]("main")
        if result:
            status = RunStatus.Success
            message = str(result.response)
        else:
            status = RunStatus.Success
            message = "No-op"
        adapter_response = {}
        if result and isinstance(result.response, dbtClassMixin):
            adapter_response = result.response.to_dict(omit_none=True)
        return RunResult(
            node=model,
            status=status,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message=message,
            adapter_response=adapter_response,
            failures=None,
            batch_results=None,
        )

    def compile(self, manifest: Manifest):
        # no-op
        return self.node

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
            self.config.project_name, "clone", self.adapter.type()
        )

        if "config" not in context:
            raise DbtInternalError(
                "Invalid materialization context generated, missing config: {}".format(context)
            )

        context_config = context["config"]

        hook_ctx = self.adapter.pre_model_hook(context_config)
        try:
            result = MacroGenerator(materialization_macro, context)()
        finally:
            self.adapter.post_model_hook(context_config, hook_ctx)

        for relation in self._materialization_relations(result, model):
            self.adapter.cache_added(relation.incorporate(dbt_created=True))

        return self._build_run_model_result(model, context)


class CloneTask(GraphRunnableTask):
    def raise_on_first_error(self) -> bool:
        return False

    def get_run_mode(self) -> GraphRunnableMode:
        return GraphRunnableMode.Independent

    def _get_deferred_manifest(self) -> Optional[Manifest]:
        # Unlike other commands, 'clone' always requires a state manifest
        # Load previous state, regardless of whether --defer flag has been set
        return self._get_previous_state()

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

                # cache the 'other' schemas too!
                if node.defer_relation:  # type: ignore
                    other_relation = adapter.Relation.create_from(
                        self.config, node.defer_relation  # type: ignore
                    )
                    result.add(other_relation.without_identifier())

        return result

    def before_run(self, adapter: BaseAdapter, selected_uids: AbstractSet[str]) -> RunStatus:
        with adapter.connection_named("master"):
            self.defer_to_manifest()
            # only create target schemas, but also cache defer_relation schemas
            schemas_to_create = super().get_model_schemas(adapter, selected_uids)
            self.create_schemas(adapter, schemas_to_create)
            schemas_to_cache = self.get_model_schemas(adapter, selected_uids)
            self.populate_adapter_cache(adapter, schemas_to_cache)
            return RunStatus.Success

    @property
    def resource_types(self) -> List[NodeType]:
        resource_types: Collection[NodeType] = resource_types_from_args(
            self.args, set(REFABLE_NODE_TYPES), set(REFABLE_NODE_TYPES)
        )

        # filter out any non-refable node types
        resource_types = [rt for rt in resource_types if rt in REFABLE_NODE_TYPES]
        return list(resource_types)

    def get_node_selector(self) -> ResourceTypeSelector:
        resource_types = self.resource_types

        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=resource_types,
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return CloneRunner
