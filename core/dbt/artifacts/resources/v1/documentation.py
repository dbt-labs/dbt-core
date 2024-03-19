from dataclasses import dataclass
from typing import Literal

from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.types import NodeType


@dataclass
class Documentation(BaseResource):
    resource_type: Literal[NodeType.Documentation]
    block_contents: str
