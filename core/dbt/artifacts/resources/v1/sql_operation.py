from dataclasses import dataclass
from typing import Literal
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import CompiledResource


@dataclass
class SqlOperation(CompiledResource):
    resource_type: Literal[NodeType.SqlOperation]
