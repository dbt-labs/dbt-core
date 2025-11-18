from dataclasses import dataclass, field
from typing import Any, List, Literal, Optional

from dbt.artifacts.resources.types import FunctionType, FunctionVolatility, NodeType
from dbt.artifacts.resources.v1.components import CompiledResource
from dbt.artifacts.resources.v1.config import NodeConfig
from dbt_common.dataclass_schema import dbtClassMixin

# =============
# Function config, and supporting classes
# =============


@dataclass
class FunctionConfig(NodeConfig):
    # The fact that this is a property, that can be changed, seems wrong.
    # A function's materialization should never be changed, so why allow for it?
    materialized: str = "function"
    type: FunctionType = FunctionType.Scalar
    volatility: Optional[FunctionVolatility] = None
    runtime_version: Optional[str] = None
    entry_point: Optional[str] = None


# =============
# Function resource, and supporting classes
# =============


@dataclass
class FunctionArgument(dbtClassMixin):
    name: str
    data_type: str
    description: Optional[str] = None
    default_value: Optional[Any] = None


@dataclass
class FunctionReturns(dbtClassMixin):
    data_type: str
    description: Optional[str] = None


@dataclass
class FunctionMandatory(dbtClassMixin):
    returns: FunctionReturns


@dataclass
class Function(CompiledResource, FunctionMandatory):
    resource_type: Literal[NodeType.Function]
    config: FunctionConfig
    arguments: List[FunctionArgument] = field(default_factory=list)
