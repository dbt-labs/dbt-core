from dataclasses import dataclass
from typing import Literal

from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import CompiledResource


@dataclass
class Analysis(CompiledResource):
    resource_type: Literal[NodeType.Analysis]
