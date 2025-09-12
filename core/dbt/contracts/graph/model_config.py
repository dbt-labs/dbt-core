from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

from dbt.artifacts.resources import (
    ExposureConfig,
    FunctionConfig,
    GroupConfig,
    MetricConfig,
    ModelConfig,
    NodeConfig,
    SavedQueryConfig,
    SeedConfig,
    SemanticModelConfig,
    SnapshotConfig,
    SourceConfig,
    TestConfig,
    UnitTestConfig,
)
from dbt.node_types import NodeType
from dbt_common.contracts.config.base import BaseConfig
from dbt_common.contracts.config.metadata import Metadata


def metas(*metas: Metadata) -> Dict[str, Any]:
    existing: Dict[str, Any] = {}
    for m in metas:
        existing = m.meta(existing)
    return existing


def insensitive_patterns(*patterns: str):
    lowercased = []
    for pattern in patterns:
        lowercased.append("".join("[{}{}]".format(s.upper(), s.lower()) for s in pattern))
    return "^({})$".format("|".join(lowercased))


@dataclass
class UnitTestNodeConfig(NodeConfig):
    expected_rows: List[Dict[str, Any]] = field(default_factory=list)
    expected_sql: Optional[str] = None


RESOURCE_TYPES: Dict[NodeType, Type[BaseConfig]] = {
    NodeType.Metric: MetricConfig,
    NodeType.SemanticModel: SemanticModelConfig,
    NodeType.SavedQuery: SavedQueryConfig,
    NodeType.Exposure: ExposureConfig,
    NodeType.Source: SourceConfig,
    NodeType.Seed: SeedConfig,
    NodeType.Test: TestConfig,
    NodeType.Model: ModelConfig,
    NodeType.Snapshot: SnapshotConfig,
    NodeType.Unit: UnitTestConfig,
    NodeType.Group: GroupConfig,
    NodeType.Function: FunctionConfig,
}


# base resource types are like resource types, except nothing has mandatory
# configs.
BASE_RESOURCE_TYPES: Dict[NodeType, Type[BaseConfig]] = RESOURCE_TYPES.copy()


def get_config_for(resource_type: NodeType, base=False) -> Type[BaseConfig]:
    if base:
        lookup = BASE_RESOURCE_TYPES
    else:
        lookup = RESOURCE_TYPES
    return lookup.get(resource_type, NodeConfig)
