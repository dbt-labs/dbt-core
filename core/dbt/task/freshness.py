import os
import threading
import time
from typing import Optional

from .base import BaseRunner
from .printer import (
    print_run_result_error,
)
from .runnable import GraphRunnableTask

from dbt.contracts.results import (
    FreshnessResult,
    PartialSourceFreshnessResult,
    SourceFreshnessResult,
    FreshnessStatus,
)
from dbt.exceptions import DbtRuntimeError, DbtInternalError
from dbt.events.functions import fire_event
from dbt.events.types import (
    FreshnessCheckComplete,
    LogStartLine,
    LogFreshnessResult,
    Note,
)
from dbt.node_types import NodeType

from dbt.adapters.capability import Capability
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.events.base_types import EventLevel
from dbt.graph import ResourceTypeSelector

RESULT_FILE_NAME = "sources.json"


class FreshnessRunner(BaseRunner):
    def on_skip(self):
        raise DbtRuntimeError("Freshness: nodes cannot be skipped!")

    def before_execute(self):
        description = "freshness of {0.source_name}.{0.name}".format(self.node)
        fire_event(
            LogStartLine(
                description=description,
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def after_execute(self, result):
        if hasattr(result, "node"):
            source_name = result.node.source_name
            table_name = result.node.name
        else:
            source_name = result.source_name
            table_name = result.table_name
        level = LogFreshnessResult.status_to_level(str(result.status))
        fire_event(
            LogFreshnessResult(
                status=result.status,
                source_name=source_name,
                table_name=table_name,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                node_info=self.node.node_info,
            ),
            level=level,
        )

    def error_result(self, node, message, start_time, timing_info):
        return self._build_run_result(
            node=node,
            start_time=start_time,
            status=FreshnessStatus.RuntimeErr,
            timing_info=timing_info,
            message=message,
        )

    def _build_run_result(self, node, start_time, status, timing_info, message):
        execution_time = time.time() - start_time
        thread_id = threading.current_thread().name
        return PartialSourceFreshnessResult(
            status=status,
            thread_id=thread_id,
            execution_time=execution_time,
            timing=timing_info,
            message=message,
            node=node,
            adapter_response={},
            failures=None,
        )

    def from_run_result(self, result, start_time, timing_info):
        result.execution_time = time.time() - start_time
        result.timing.extend(timing_info)
        return result

    def execute(self, compiled_node, manifest):
        relation = self.adapter.Relation.create_from_source(compiled_node)
        # given a Source, calculate its freshness.
        with self.adapter.connection_for(compiled_node):
            self.adapter.clear_transaction()
            adapter_response: Optional[AdapterResponse] = None
            freshness = None

            if compiled_node.loaded_at_field is not None:
                adapter_response, freshness = self.adapter.calculate_freshness(
                    relation,
                    compiled_node.loaded_at_field,
                    compiled_node.freshness.filter,
                    manifest=manifest,
                )

                status = compiled_node.freshness.status(freshness["age"])
            elif self.adapter.supports(Capability.TableLastModifiedMetadata):
                if compiled_node.freshness.filter is not None:
                    fire_event(
                        Note(
                            msg=f"A filter cannot be applied to a metadata freshness check on source '{compiled_node.name}'."
                        ),
                        EventLevel.WARN,
                    )

                adapter_response, freshness = self.adapter.calculate_freshness_from_metadata(
                    relation,
                    manifest=manifest,
                )

                status = compiled_node.freshness.status(freshness["age"])
            else:
                raise DbtRuntimeError(
                    f"Could not compute freshness for source {compiled_node.name}: no 'loaded_at_field' provided and {self.adapter.type()} adapter does not support metadata-based freshness checks."
                )

        # adapter_response was not returned in previous versions, so this will be None
        # we cannot call to_dict() on NoneType
        if adapter_response:
            adapter_response = adapter_response.to_dict(omit_none=True)

        return SourceFreshnessResult(
            node=compiled_node,
            status=status,
            thread_id=threading.current_thread().name,
            timing=[],
            execution_time=0,
            message=None,
            adapter_response=adapter_response or {},
            failures=None,
            **freshness,
        )

    def compile(self, manifest):
        if self.node.resource_type != NodeType.Source:
            # should be unreachable...
            raise DbtRuntimeError("fresnhess runner: got a non-Source")
        # we don't do anything interesting when we compile a source node
        return self.node


class FreshnessSelector(ResourceTypeSelector):
    def node_is_match(self, node):
        if not super().node_is_match(node):
            return False
        if not isinstance(node, SourceDefinition):
            return False
        return node.has_freshness


class FreshnessTask(GraphRunnableTask):
    def defer_to_manifest(self, adapter, selected_uids):
        # freshness don't defer
        return

    def result_path(self):
        if self.args.output:
            return os.path.realpath(self.args.output)
        else:
            return os.path.join(self.config.project_target_path, RESULT_FILE_NAME)

    def raise_on_first_error(self):
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return FreshnessSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Source],
        )

    def get_runner_type(self, _):
        return FreshnessRunner

    def get_result(self, results, elapsed_time, generated_at):
        return FreshnessResult.from_node_results(
            elapsed_time=elapsed_time, generated_at=generated_at, results=results
        )

    def task_end_messages(self, results):
        for result in results:
            if result.status in (FreshnessStatus.Error, FreshnessStatus.RuntimeErr):
                print_run_result_error(result)

        fire_event(FreshnessCheckComplete())
