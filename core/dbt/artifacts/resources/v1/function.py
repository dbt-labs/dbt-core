from dataclasses import dataclass, field
from typing import List, Literal, Optional

from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import CompiledResource
from dbt.artifacts.resources.v1.config import NodeConfig
from dbt_common.dataclass_schema import dbtClassMixin

# =============
# Function config, and supporting classes
# =============


@dataclass
class FunctionConfig(NodeConfig):
    pass


# =============
# Function resource, and supporting classes
# =============


@dataclass
class FunctionArgument(dbtClassMixin):
    name: str
    type: str
    description: Optional[str] = None


@dataclass
class FunctionReturnType(dbtClassMixin):
    type: str
    description: Optional[str] = None


@dataclass
class FunctionMandatory(dbtClassMixin):
    return_type: FunctionReturnType


@dataclass
class Function(CompiledResource, FunctionMandatory):
    resource_type: Literal[NodeType.Function]
    config: FunctionConfig
    arguments: List[FunctionArgument] = field(default_factory=list)
