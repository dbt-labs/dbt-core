from dataclasses import dataclass
from typing import Literal

from dbt.artifacts.contracts.base import BaseArtifactNode
from dbt.artifacts.contracts.types import NodeType


@dataclass
class Documentation(BaseArtifactNode):
    resource_type: Literal[NodeType.Documentation]
    block_contents: str
