import json
from typing import Type

from dbt.graph import (
    parse_difference,
    ResourceTypeSelector,
    SelectionSpec,
)
from dbt.task.runnable import GraphRunnableTask, ManifestTask
from dbt.task.test import TestSelector
from dbt.node_types import NodeType
from dbt.exceptions import RuntimeException, InternalException
from dbt.logger import log_manager, GLOBAL_LOGGER as logger


class ListTask(GraphRunnableTask):
    DEFAULT_RESOURCE_VALUES = frozenset((
        NodeType.Model,
        NodeType.Snapshot,
        NodeType.Seed,
        NodeType.Test,
        NodeType.Source,
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
    ))

    def __init__(self, args, config):
        super().__init__(args, config)
        self.args.single_threaded = True
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
        log_manager.stderr_console()
        super().pre_init_hook(args)

    def _iterate_selected_nodes(self):
        selector = self.get_node_selector()
        spec = self.get_selection_spec()
        nodes = sorted(selector.get_selected(spec))
        if not nodes:
            logger.warning('No nodes selected!')
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
            else:
                raise RuntimeException(
                    f'Got an unexpected result from node selection: "{node}"'
                    f'Expected a source or a node!'
                )

    def generate_selectors(self):
        for node in self._iterate_selected_nodes():
            selector = '.'.join(node.fqn)
            if node.resource_type == NodeType.Source:
                yield 'source:{}'.format(selector)
            else:
                yield selector

    def generate_names(self):
        for node in self._iterate_selected_nodes():
            if node.resource_type == NodeType.Source:
                yield '{0.source_name}.{0.name}'.format(node)
            else:
                yield node.name

    def generate_json(self):
        for node in self._iterate_selected_nodes():
            yield json.dumps({
                k: v
                for k, v in node.to_dict(omit_none=False).items()
                if k in self.ALLOWED_KEYS
            })

    def generate_paths(self):
        for node in self._iterate_selected_nodes():
            yield node.original_file_path

    def run(self):
        ManifestTask._runtime_initialize(self)
        output = self.config.args.output
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
        for result in generator():
            self.node_results.append(result)
            print(result)
        return self.node_results

    @property
    def resource_types(self):
        if self.args.models:
            return [NodeType.Model]

        values = set(self.config.args.resource_types)
        if not values:
            return list(self.DEFAULT_RESOURCE_VALUES)

        if 'default' in values:
            values.remove('default')
            values.update(self.DEFAULT_RESOURCE_VALUES)
        if 'all' in values:
            values.remove('all')
            values.update(self.ALL_RESOURCE_VALUES)
        return list(values)

    @property
    def selector(self):
        if self.args.models:
            return self.args.models
        else:
            return self.args.select

    def get_selection_spec(self) -> SelectionSpec:
        spec = parse_difference(self.selector, self.args.exclude)
        return spec

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        cls: Type[ResourceTypeSelector]
        if self.resource_types == [NodeType.Test]:
            return TestSelector(
                graph=self.graph,
                manifest=self.manifest,
            )
        else:
            return ResourceTypeSelector(
                graph=self.graph,
                manifest=self.manifest,
                resource_types=self.resource_types,
            )

    def interpret_results(self, results):
        return bool(results)
