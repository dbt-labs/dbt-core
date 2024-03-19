from dataclasses import dataclass
from typing import Optional, Literal
from dbt.artifacts.resources.v1.components import CompiledResource
from dbt.artifacts.resources.types import NodeType


@dataclass
class HookNode(CompiledResource):
    resource_type: Literal[NodeType.Operation]
    index: Optional[int] = None
