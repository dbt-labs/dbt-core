import os
import threading
import time
from typing import Dict

from .base import BaseRunner
from .printer import (
    print_start_line,
    print_freshness_result_line,
    print_run_result_error,
)
from .runnable import GraphRunnableTask

from dbt.contracts.results import (
    FreshnessExecutionResult,
    SourceFreshnessResult,
    PartialResult,
)
from dbt.exceptions import RuntimeException, InternalException
from dbt.logger import print_timestamped_line
from dbt.node_types import NodeType

from dbt import utils

from dbt.graph import NodeSelector, SelectionSpec, parse_difference
from dbt.contracts.graph.parsed import ParsedSourceDefinition


RESULT_FILE_NAME = 'sources.json'


class FreshnessRunner(BaseRunner):
    def on_skip(self):
        raise RuntimeException(
            'Freshness: nodes cannot be skipped!'
        )

    def get_result_status(self, result) -> Dict[str, str]:
        if result.error:
            return {'node_status': 'error', 'node_error': str(result.error)}
        else:
            return {'node_status': str(result.status)}

    def before_execute(self):
        description = 'freshness of {0.source_name}.{0.name}'.format(self.node)
        print_start_line(description, self.node_index, self.num_nodes)

    def after_execute(self, result):
        print_freshness_result_line(result, self.node_index, self.num_nodes)

    def _build_run_result(self, node, start_time, error, status, timing_info,
                          skip=False, failed=None):
        execution_time = time.time() - start_time
        thread_id = threading.current_thread().name
        status = utils.lowercase(status)
        return PartialResult(
            node=node,
            status=status,
            error=error,
            execution_time=execution_time,
            thread_id=thread_id,
            timing=timing_info,
        )

    def from_run_result(self, result, start_time, timing_info):
        result.execution_time = (time.time() - start_time)
        result.timing.extend(timing_info)
        return result

    def execute(self, compiled_node, manifest):
        # we should only be here if we compiled_node.has_freshness, and
        # therefore loaded_at_field should be a str. If this invariant is
        # broken, raise!
        if compiled_node.loaded_at_field is None:
            raise InternalException(
                'Got to execute for source freshness of a source that has no '
                'loaded_at_field!'
            )

        relation = self.adapter.Relation.create_from_source(compiled_node)
        # given a Source, calculate its fresnhess.
        with self.adapter.connection_for(compiled_node):
            self.adapter.clear_transaction()
            freshness = self.adapter.calculate_freshness(
                relation,
                compiled_node.loaded_at_field,
                compiled_node.freshness.filter,
                manifest=manifest
            )

        status = compiled_node.freshness.status(freshness['age'])

        return SourceFreshnessResult(
            node=compiled_node,
            status=status,
            thread_id=threading.current_thread().name,
            **freshness
        )

    def compile(self, manifest):
        if self.node.resource_type != NodeType.Source:
            # should be unreachable...
            raise RuntimeException('fresnhess runner: got a non-Source')
        # we don't do anything interesting when we compile a source node
        return self.node


class FreshnessSelector(NodeSelector):
    def node_is_match(self, node):
        if not super().node_is_match(node):
            return False
        if not isinstance(node, ParsedSourceDefinition):
            return False
        return node.has_freshness


class FreshnessTask(GraphRunnableTask):
    def result_path(self):
        if self.args.output:
            return os.path.realpath(self.args.output)
        else:
            return os.path.join(self.config.target_path, RESULT_FILE_NAME)

    def raise_on_first_error(self):
        return False

    def get_selection_spec(self) -> SelectionSpec:
        include = [
            'source:{}'.format(s)
            for s in (self.args.selected or ['*'])
        ]
        spec = parse_difference(include, None)
        return spec

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        return FreshnessSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
        )

    def get_runner_type(self):
        return FreshnessRunner

    def get_result(self, results, elapsed_time, generated_at):
        return FreshnessExecutionResult(
            elapsed_time=elapsed_time,
            generated_at=generated_at,
            results=results
        )

    def task_end_messages(self, results):
        for result in results:
            if result.error is not None:
                print_run_result_error(result)

        print_timestamped_line('Done.')
