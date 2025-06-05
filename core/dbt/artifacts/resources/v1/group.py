from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.owner import Owner
from dbt_common.contracts.config.base import BaseConfig, MergeBehavior


@dataclass
class GroupConfig(BaseConfig):
    meta: Dict[str, Any] = field(default_factory=dict, metadata=MergeBehavior.Update.meta())


@dataclass
class Group(BaseResource):
    name: str
    owner: Owner
    resource_type: Literal[NodeType.Group]
    description: Optional[str] = None
    config: GroupConfig = field(default_factory=GroupConfig)
