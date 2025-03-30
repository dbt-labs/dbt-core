from dataclasses import dataclass, field
from typing import Dict

from dbt.artifacts.resources import NodeVersion  # noqa

# all these are just exports, they need "noqa" so flake8 will not complain.
from dbt.contracts.graph.manifest import Manifest  # noqa
from dbt.contracts.graph.node_args import ModelNodeArgs
from dbt.graph.graph import UniqueId  # noqa
from dbt.node_types import AccessType, NodeType  # noqa


@dataclass
class PluginNodes:
    models: Dict[str, ModelNodeArgs] = field(default_factory=dict)

    def add_model(self, model_args: ModelNodeArgs) -> None:
        self.models[model_args.unique_id] = model_args

    def update(self, other: "PluginNodes") -> None:
        self.models.update(other.models)
