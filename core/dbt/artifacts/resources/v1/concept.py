from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from dbt.artifacts.resources.base import GraphResource
from dbt.artifacts.resources.v1.components import DependsOn
from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class ConceptJoin(dbtClassMixin):
    """Represents a join relationship in a concept definition."""

    name: str  # name of the model or concept to join
    base_key: str  # column in base model for join
    foreign_key: Optional[str] = None  # column in joined model (defaults to primary_key)
    alias: Optional[str] = None  # alias for the joined table
    columns: List[str] = field(default_factory=list)  # columns to expose from join
    join_type: str = "left"  # type of join (left, inner, etc.)


@dataclass
class ConceptColumn(dbtClassMixin):
    """Represents a column definition in a concept."""

    name: str
    description: Optional[str] = None
    alias: Optional[str] = None  # optional alias for the column


@dataclass
class ConceptConfig(dbtClassMixin):
    """Configuration for a concept."""

    enabled: bool = True
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Concept(GraphResource):
    """A concept resource definition."""

    name: str
    base_model: str  # reference to the base model
    description: str = ""
    primary_key: Union[str, List[str]] = "id"  # primary key column(s)
    columns: List[ConceptColumn] = field(default_factory=list)
    joins: List[ConceptJoin] = field(default_factory=list)
    config: ConceptConfig = field(default_factory=ConceptConfig)
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    depends_on: DependsOn = field(default_factory=DependsOn)


# Type alias for concept resource
ConceptResource = Concept
