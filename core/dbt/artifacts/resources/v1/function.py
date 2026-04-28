from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from dbt.artifacts.resources.types import FunctionType, FunctionVolatility, NodeType
from dbt.artifacts.resources.v1.components import CompiledResource, HasRelationMetadata
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
class DeferFunction(HasRelationMetadata):
    alias: str
    resource_type: NodeType
    name: str
    description: str
    compiled_code: Optional[str]
    meta: Dict[str, Any]
    tags: List[str]
    config: Optional[FunctionConfig]
    arguments: List[FunctionArgument]
    returns: FunctionReturns

    @property
    def identifier(self):
        return self.name


@dataclass
class FunctionOverload(dbtClassMixin):
    """An overload of a function with different argument signatures.

    Each overload references a separate SQL file (via defined_in) that
    contains the function body for this overload. All overloads share the
    same database function name but have different argument types.
    """

    defined_in: str
    arguments: List[FunctionArgument] = field(default_factory=list)
    returns: Optional[FunctionReturns] = None
    description: Optional[str] = None
    # Populated during patch processing from the overload's SQL file
    raw_body: Optional[str] = None
    # Populated during compile by the FunctionRunner
    compiled_body: Optional[str] = None


@dataclass
class Function(CompiledResource, FunctionMandatory):
    resource_type: Literal[NodeType.Function]
    config: FunctionConfig
    arguments: List[FunctionArgument] = field(default_factory=list)
    overloads: List[FunctionOverload] = field(default_factory=list)
    defer_function: Optional[DeferFunction] = None

    def __post_serialize__(self, dct: Dict, context: Optional[Dict] = None):
        dct = super().__post_serialize__(dct, context)
        if context and context.get("artifact") and "defer_function" in dct:
            del dct["defer_function"]
        return dct
