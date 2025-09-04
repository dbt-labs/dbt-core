import threading
from typing import Any

from dbt.adapters.exceptions import MissingMaterializationError
from dbt.artifacts.schemas.results import NodeStatus, RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.clients.jinja import MacroGenerator
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import LogNodeResult, LogStartLine
from dbt.task import group_lookup
from dbt.task.compile import CompileRunner
from dbt_common.clients.jinja import MacroProtocol
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtValidationError


class FunctionRunner(CompileRunner):

    def __init__(self, config, adapter, node, node_index: int, num_nodes: int) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)

        # doing this gives us type hints for the node :D
        assert isinstance(node, FunctionNode)
        self.node = node

    def describe_node(self) -> str:
        return f"function {self.node.name}"  # TODO: add more info, similar to SeedRunner.describe_node

    def before_execute(self) -> None:
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def _get_materialization_macro(
        self, compiled_node: FunctionNode, manifest: Manifest
    ) -> MacroProtocol:
        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name, compiled_node.get_materialization(), self.adapter.type()
        )
        if materialization_macro is None:
            raise MissingMaterializationError(
                materialization=compiled_node.get_materialization(),
                adapter_type=self.adapter.type(),
            )

        return materialization_macro

    def _check_lang_supported(
        self, compiled_node: FunctionNode, materialization_macro: MacroProtocol
    ) -> None:
        # TODO: This function and its typing is a bit wonky, we should fix it
        # Specifically, a MacroProtocol doesn't have a supported_languags attribute, but a macro does. We're acting
        # like the materialization_macro might not have a supported_languages attribute, but we access it in an unguarded manner.
        # So are we guaranteed to always have a Macro here? (because a Macro always has a supported_languages attribute)
        # This logic is a copy of of the logic in the run.py file, so the same logical conundrum applies there. Also perhaps
        # we can refactor to having one definition, and maybe a logically consistent one...
        mat_has_supported_langs = hasattr(materialization_macro, "supported_languages")
        function_lang_supported = compiled_node.language in materialization_macro.supported_languages  # type: ignore
        if mat_has_supported_langs and not function_lang_supported:
            str_langs = [str(lang) for lang in materialization_macro.supported_languages]  # type: ignore
            raise DbtValidationError(
                f'Materialization "{materialization_macro.name}" only supports languages {str_langs}; '
                f'got "{compiled_node.language}"'
            )

    def build_result(self, compiled_node: FunctionNode, result: Any) -> RunResult:
        adapter_response = {}
        response = result.response
        if isinstance(response, dbtClassMixin):
            adapter_response = response.to_dict(omit_none=True)

        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0.0,  # TODO: add execution time
            message=str(result.response),
            adapter_response=adapter_response,
            failures=result.get("failures"),
            batch_results=None,
        )

    def execute(self, compiled_node: FunctionNode, manifest: Manifest) -> RunResult:
        materialization_macro = self._get_materialization_macro(compiled_node, manifest)
        self._check_lang_supported(compiled_node, materialization_macro)

        result = MacroGenerator(
            materialization_macro, {}
        )()  # TODO: Should we be passing in a context here? If so, what should be in it?

        # TODO: Should we be caching something here?
        # for relation in self._materialization_relations(result, model):
        #     self.adapter.cache_added(relation.incorporate(dbt_created=True))

        return self.build_result(compiled_node, result)

    def after_execute(self, result: RunResult) -> None:
        pass  # TODO: add after_execute logic to print the result

    # def compile() defined on CompileRunner

    def print_result_line(self, result: RunResult) -> None:
        node = result.node
        assert isinstance(node, FunctionNode)

        group = group_lookup.get(node.unique_id)
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogNodeResult(  # TODO: rename to LogFunctionResult
                status=result.status,
                result_message=result.message,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                schema=self.node.schema,
                relation=node.alias,
                node_info=node.node_info,
                group=group,
            ),
            level=level,
        )
