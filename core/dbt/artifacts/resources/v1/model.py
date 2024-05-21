from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Literal, Optional

from dbt.artifacts.resources.types import AccessType, NodeType
from dbt.artifacts.resources.v1.components import (
    CompiledResource,
    DeferRelation,
    NodeVersion,
)
from dbt.artifacts.resources.v1.config import NodeConfig
from dbt_common.contracts.config.base import MergeBehavior
from dbt_common.contracts.constraints import ModelLevelConstraint


@dataclass
class ModelConfig(NodeConfig):
    access: AccessType = field(
        default=AccessType.Protected,
        metadata=MergeBehavior.Clobber.meta(),
    )


@dataclass
class Model(CompiledResource):
    resource_type: Literal[NodeType.Model]
    access: AccessType = AccessType.Protected
    config: ModelConfig = field(default_factory=ModelConfig)
    constraints: List[ModelLevelConstraint] = field(default_factory=list)
    version: Optional[NodeVersion] = None
    latest_version: Optional[NodeVersion] = None
    deprecation_date: Optional[datetime] = None
    defer_relation: Optional[DeferRelation] = None
    primary_key: List[str] = field(default_factory=list)

    def __post_serialize__(self, dct: Dict, context: Optional[Dict] = None):
        dct = super().__post_serialize__(dct, context)
        if context and context.get("artifact") and "defer_relation" in dct:
            del dct["defer_relation"]
        return dct
