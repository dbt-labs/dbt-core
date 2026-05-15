import itertools
import threading
from typing import Optional, Set, Type, TypeVar

from dbt.artifacts.schemas.run import RunResult, RunStatus
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ManifestSQLNode
from dbt.events.types import CompiledNode, ParseInlineNodeError
from dbt.flags import get_flags
from dbt.graph import ResourceTypeSelector
from dbt.graph.cli import INTERSECTION_DELIMITER
from dbt.graph.graph import UniqueId
from dbt.graph.selector_spec import SelectionCriteria
from dbt.node_types import EXECUTABLE_NODE_TYPES, NodeType
from dbt.parser.manifest import process_node
from dbt.parser.sql import SqlBlockParser
from dbt.task.base import BaseRunner
from dbt.task.runnable import GraphRunnableTask
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note
from dbt_common.exceptions import CompilationError
from dbt_common.exceptions import DbtBaseException as DbtException
from dbt_common.exceptions import DbtInternalError

CompilableNodeT = TypeVar("CompilableNodeT", bound=ManifestSQLNode)


class CompileRunner(BaseRunner[CompilableNodeT, RunResult]):
    def before_execute(self) -> None:
        pass

    def after_execute(self, result: RunResult) -> None:
        pass

    def execute(self, compiled_node, manifest) -> RunResult:
        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message=None,
            adapter_response={},
            failures=None,
            batch_results=None,
        )

    def compile(self, manifest: Manifest):
        return self.compiler.compile_node(self.node, manifest, {})

    def get_node_representation(self):
        display_quote_policy = {"database": False, "schema": False, "identifier": False}
        relation = self.adapter.Relation.create_from(
            self.config, self.node, quote_policy=display_quote_policy
        )
        # exclude the database from output if it's the default
        if self.node.database == self.config.credentials.database:
            relation = relation.include(database=False)
        return str(relation)


class CompileTask(GraphRunnableTask):
    # We add a new inline node to the manifest during initialization
    # it should be removed before the task is complete
    _inline_node_id = None

    def raise_on_first_error(self) -> bool:
        return True

    def get_node_selector(self) -> ResourceTypeSelector:
        if getattr(self.args, "inline", None):
            resource_types = [NodeType.SqlOperation]
        else:
            resource_types = EXECUTABLE_NODE_TYPES

        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=resource_types,
            selectors=self.config.selectors,
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return CompileRunner

    def _get_directly_selected_unique_ids(self) -> Set[UniqueId]:
        """unique_ids matched by --select before graph operator (+/@) expansion.

        Mirrors the tokenization logic in ``parse_union``: space-splits each
        entry into independent specs, then comma-splits each spec into an
        intersection group.  Each criterion is resolved independently via
        ``select_included`` (the no-operator-expansion path); intersection
        groups are intersected, and the results are unioned.
        """
        if not self.selection_arg:
            return set()
        selector = self.get_node_selector()
        graph_nodes = selector.graph.nodes()
        result: Set[UniqueId] = set()
        raw_specs = itertools.chain.from_iterable(r.split(" ") for r in self.selection_arg)
        for raw_spec in raw_specs:
            parts = raw_spec.split(INTERSECTION_DELIMITER)
            part_sets = [
                selector.select_included(graph_nodes, SelectionCriteria.from_single_spec(part))
                for part in parts
            ]
            if part_sets:
                result |= set.intersection(*part_sets)
        return result

    def task_end_messages(self, results) -> None:
        is_inline = bool(getattr(self.args, "inline", None))
        output_format = getattr(self.args, "output", "text")

        if is_inline:
            matched_results = [result for result in results if result.node.name == "inline_query"]
        elif self.selection_arg:
            directly_selected = self._get_directly_selected_unique_ids()
            matched_results = []
            for result in results:
                if result.node.unique_id in directly_selected:
                    matched_results.append(result)
                else:
                    fire_event(
                        Note(msg=f"Excluded node '{result.node.name}' from results"),
                        EventLevel.DEBUG,
                    )
        # No selector passed, compiling all nodes
        else:
            matched_results = []

        for result in matched_results:
            fire_event(
                CompiledNode(
                    node_name=result.node.name,
                    compiled=result.node.compiled_code,
                    is_inline=is_inline,
                    output_format=output_format,
                    unique_id=result.node.unique_id,
                    quiet=get_flags().QUIET,
                )
            )

    def _runtime_initialize(self):
        if getattr(self.args, "inline", None):
            try:
                block_parser = SqlBlockParser(
                    project=self.config, manifest=self.manifest, root_project=self.config
                )
                sql_node = block_parser.parse_remote(self.args.inline, "inline_query")
                process_node(self.config, self.manifest, sql_node)
                # Special hack to remove disabled, if it's there. This would only happen
                # if all models are disabled in dbt_project
                if sql_node.config.enabled is False:
                    sql_node.config.enabled = True
                    self.manifest.disabled.pop(sql_node.unique_id)
                    self.manifest.nodes[sql_node.unique_id] = sql_node
                # keep track of the node added to the manifest
                self._inline_node_id = sql_node.unique_id
            except CompilationError as exc:
                fire_event(
                    ParseInlineNodeError(
                        exc=str(exc.msg),
                        node_info={
                            "node_path": "sql/inline_query",
                            "node_name": "inline_query",
                            "unique_id": "sqloperation.test.inline_query",
                            "node_status": "failed",
                        },
                    )
                )
                raise DbtException("Error parsing inline query")
        super()._runtime_initialize()

    def after_run(self, adapter, results) -> None:
        # remove inline node from manifest
        if self._inline_node_id:
            self.manifest.nodes.pop(self._inline_node_id)
            self._inline_node_id = None
        super().after_run(adapter, results)

    def _handle_result(self, result) -> None:
        super()._handle_result(result)

        if (
            result.node.is_ephemeral_model
            and type(self) is CompileTask
            and (self.args.select or getattr(self.args, "inline", None))
        ):
            self.node_results.append(result)
