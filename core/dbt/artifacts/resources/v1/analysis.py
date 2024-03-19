from dbt.artifacts.resources.v1.components import CompiledResource
from typing import Literal
from dataclasses import dataclass
from dbt.artifacts.resources.types import NodeType


@dataclass
class Analysis(CompiledResource):
    resource_type: Literal[NodeType.Analysis]
