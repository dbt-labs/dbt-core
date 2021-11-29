import json

from dbt.contracts.graph.parsed import (
    ParsedExposure,
    ParsedSourceDefinition,
    ParsedMetric
)
from dbt.graph import ResourceTypeSelector
from dbt.task.runnable import GraphRunnableTask, ManifestTask
from dbt.task.test import TestSelector
from dbt.node_types import NodeType
from dbt.exceptions import RuntimeException, InternalException, warn_or_error
from dbt.logger import log_manager
import logging
import dbt.events.functions as event_logger


class ListTask(GraphRunnableTask):
    DEFAULT_RESOURCE_VALUES = frozenset((
        NodeType.Model,
        NodeType.Snapshot,
        NodeType.Seed,
        NodeType.Test,
        NodeType.Source,
        NodeType.Exposure,
        NodeType.Metric,
    ))
    ALL_RESOURCE_VALUES = DEFAULT_RESOURCE_VALUES | frozenset((
        NodeType.Analysis,
    ))
    ALLOWED_KEYS = frozenset((
        'alias',
        'name',
        'package_name',
        'depends_on',
        'tags',
        'config',
        'resource_type',
        'source_name',
        'original_file_path',
        'unique_id'
    ))

    def __init__(self, args, config):
        super().__init__(args, config)
        if self.args.models:
            if self.args.select:
                raise RuntimeException(
                    '"models" and "select" are mutually exclusive arguments'
                )
            if self.args.resource_types:
                raise RuntimeException(
                    '"models" and "resource_type" are mutually exclusive '
                    'arguments'
                )

    @classmethod
    def pre_init_hook(cls, args):
        """A hook called before the task is initialized."""
        # Filter out all INFO-level logging to allow piping ls output to jq, etc
        # WARN level will still include all warnings + errors
        # Do this by:
        #  - returning the log level so that we can pass it into the 'level_override'
        #    arg of events.functions.setup_event_logger() -- good!
        #  - mutating the initialized, not-yet-configured STDOUT event logger
        #    because it's being configured too late -- bad! TODO refactor!
        log_manager.stderr_console()
        event_logger.STDOUT_LOG.level = logging.WARN
        super().pre_init_hook(args)
        return logging.WARN

    def _iterate_selected_nodes(self):
        selector = self.get_node_selector()
        spec = self.get_selection_spec()
        nodes = sorted(selector.get_selected(spec))
        if not nodes:
            warn_or_error('No nodes selected!')
            return
        if self.manifest is None:
            raise InternalException(
                'manifest is None in _iterate_selected_nodes'
            )
        for node in nodes:
            if node in self.manifest.nodes:
                yield self.manifest.nodes[node]
            elif node in self.manifest.sources:
                yield self.manifest.sources[node]
            elif node in self.manifest.exposures:
                yield self.manifest.exposures[node]
            elif node in self.manifest.metrics:
                yield self.manifest.metrics[node]
            else:
                raise RuntimeException(
                    f'Got an unexpected result from node selection: "{node}"'
                    f'Expected a source or a node!'
                )

    def generate_selectors(self):
        for node in self._iterate_selected_nodes():
            if node.resource_type == NodeType.Source:
                assert isinstance(node, ParsedSourceDefinition)
                # sources are searched for by pkg.source_name.table_name
                source_selector = '.'.join([
                    node.package_name, node.source_name, node.name
                ])
                yield f'source:{source_selector}'
            elif node.resource_type == NodeType.Exposure:
                assert isinstance(node, ParsedExposure)
                # exposures are searched for by pkg.exposure_name
                exposure_selector = '.'.join([node.package_name, node.name])
                yield f'exposure:{exposure_selector}'
            elif node.resource_type == NodeType.Metric:
                assert isinstance(node, ParsedMetric)
                # metrics are searched for by pkg.metric_name
                metric_selector = '.'.join([node.package_name, node.name])
                yield f'metric:{metric_selector}'
            else:
                # everything else is from `fqn`
                yield '.'.join(node.fqn)

    def generate_names(self):
        for node in self._iterate_selected_nodes():
            yield node.search_name

    def generate_json(self):
        for node in self._iterate_selected_nodes():
            yield json.dumps({
                k: v
                for k, v in node.to_dict(omit_none=False).items()
                if (
                    k in self.args.output_keys
                    if self.args.output_keys is not None
                    else k in self.ALLOWED_KEYS
                )
            })

    def generate_paths(self):
        for node in self._iterate_selected_nodes():
            yield node.original_file_path

    def run(self):
        ManifestTask._runtime_initialize(self)
        output = self.args.output
        if output == 'selector':
            generator = self.generate_selectors
        elif output == 'name':
            generator = self.generate_names
        elif output == 'json':
            generator = self.generate_json
        elif output == 'path':
            generator = self.generate_paths
        else:
            raise InternalException(
                'Invalid output {}'.format(output)
            )

        return self.output_results(generator())

    def output_results(self, results):
        for result in results:
            self.node_results.append(result)
            print(result)
        return self.node_results

    @property
    def resource_types(self):
        if self.args.models:
            return [NodeType.Model]

        if not self.args.resource_types:
            return list(self.DEFAULT_RESOURCE_VALUES)

        values = set(self.args.resource_types)
        if 'default' in values:
            values.remove('default')
            values.update(self.DEFAULT_RESOURCE_VALUES)
        if 'all' in values:
            values.remove('all')
            values.update(self.ALL_RESOURCE_VALUES)
        return list(values)

    @property
    def selection_arg(self):
        # for backwards compatibility, list accepts both --models and --select,
        # with slightly different behavior: --models implies --resource-type model
        if self.args.models:
            return self.args.models
        else:
            return self.args.select

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        if self.resource_types == [NodeType.Test]:
            return TestSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
            )
        else:
            return ResourceTypeSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
                resource_types=self.resource_types,
            )

    def interpret_results(self, results):
        # list command should always return 0 as exit code
        return True
