from .compile import CompileTask

from .freshness import FreshnessRunner as freshness_runner
from .run import ModelRunner as run_model_runner
from .snapshot import SnapshotRunner as snapshot_model_runner
from .seed import SeedRunner as seed_runner
from .test import TestRunner as test_runner

from dbt.graph import ResourceTypeSelector
from dbt.exceptions import InternalException
from dbt.node_types import NodeType


class BuildTask(CompileTask):
    """
    Build task.  It really ties the room together. 
    """
    
    RUNNER_MAP = {
        NodeType.Model: run_model_runner,
        NodeType.Snapshot: snapshot_model_runner,
        NodeType.Seed: seed_runner,
        NodeType.Test: test_runner,
        #NodeType.Source: freshness_runner
    }

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )

        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[x for x in self.RUNNER_MAP.keys()],
        )
    
    def get_runner_type(self, node):
        return self.RUNNER_MAP.get(node.resource_type)
