from .run import RunTask, ModelRunner as run_model_runner
from .snapshot import SnapshotRunner as snapshot_model_runner
from .seed import SeedRunner as seed_runner
from .test import TestRunner as test_runner

from dbt.contracts.results import NodeStatus
from dbt.exceptions import RuntimeException, InternalException
from dbt.graph import (
    parse_difference,
    ResourceTypeSelector,
    SelectionSpec,
)
from dbt.node_types import NodeType
from dbt.task.test import TestSelector


class BuildTask(RunTask):
    """The Build task processes all assets of a given process and attempts to
    'build' them in an opinionated fashion.  Every resource type outlined in
    RUNNER_MAP will be processed by the mapped runner class.

    I.E. a resource of type Model is handled by the ModelRunner which is
    imported as run_model_runner. """

    MARK_DEPENDENT_ERRORS_STATUSES = [NodeStatus.Error, NodeStatus.Fail]

    RUNNER_MAP = {
        NodeType.Model: run_model_runner,
        NodeType.Snapshot: snapshot_model_runner,
        NodeType.Seed: seed_runner,
        NodeType.Test: test_runner,
    }
    ALL_RESOURCE_VALUES = frozenset({x for x in RUNNER_MAP.keys()})

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

    @property
    def resource_types(self):
        if self.args.models:
            return [NodeType.Model]

        if not self.args.resource_types:
            return list(self.ALL_RESOURCE_VALUES)

        values = set(self.args.resource_types)

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
        if self.args.selector_name:
            spec = self.config.get_selector(self.args.selector_name)
        else:
            spec = parse_difference(self.selector, self.args.exclude)
        return spec

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get node selection'
            )

        resource_types = self.resource_types

        if resource_types == [NodeType.Test]:
            return TestSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=resource_types,
        )

    def get_runner_type(self, node):
        return self.RUNNER_MAP.get(node.resource_type)
