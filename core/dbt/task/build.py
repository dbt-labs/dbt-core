from .compile import CompileTask

from .run import ModelRunner as run_model_runner
from .snapshot import SnapshotRunner as snapshot_model_runner
from .seed import SeedRunner as seed_runner
from .test import TestRunner as test_runner

from dbt.graph import ResourceTypeSelector
from dbt.exceptions import InternalException
from dbt.node_types import NodeType


class BuildTask(CompileTask):
    """
    The Build task processes all assets of a given process and attempts to 'build'
    them in an opinionated fashion.  Every resource type outlined in RUNNER_MAP
    will be processed by the mapped runner class.

    I.E. a resource of type Model is handled by the ModelRunner which is imported
    as run_model_runner.
    """

    # TODO: is this list complete?
    RUNNER_MAP = {
        NodeType.Model: run_model_runner,
        NodeType.Snapshot: snapshot_model_runner,
        NodeType.Seed: seed_runner,
        NodeType.Test: test_runner,
    }

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get node selection'
            )

        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[x for x in self.RUNNER_MAP.keys()],
        )

    def get_runner_type(self, node):
        return self.RUNNER_MAP.get(node.resource_type)
