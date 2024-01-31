from dataclasses import dataclass, field
from dbt.artifacts.resources.v1.macro import MacroDependsOn
from dbt_common.contracts.config.properties import AdditionalPropertiesMixin
from dbt_common.contracts.constraints import ColumnLevelConstraint
from dbt_common.contracts.util import Replaceable
from dbt_common.dataclass_schema import dbtClassMixin, ExtensibleDbtClassMixin
from typing import Any, Dict, List, Optional, Union


NodeVersion = Union[str, float]


@dataclass
class DependsOn(MacroDependsOn):
    nodes: List[str] = field(default_factory=list)

    def add_node(self, value: str):
        if value not in self.nodes:
            self.nodes.append(value)


@dataclass
class RefArgs(dbtClassMixin):
    name: str
    package: Optional[str] = None
    version: Optional[NodeVersion] = None

    @property
    def positional_args(self) -> List[str]:
        if self.package:
            return [self.package, self.name]
        else:
            return [self.name]

    @property
    def keyword_args(self) -> Dict[str, Optional[NodeVersion]]:
        if self.version:
            return {"version": self.version}
        else:
            return {}


@dataclass
class ColumnInfo(AdditionalPropertiesMixin, ExtensibleDbtClassMixin, Replaceable):
    """Used in all ManifestNodes and SourceDefinition"""

    name: str
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    data_type: Optional[str] = None
    constraints: List[ColumnLevelConstraint] = field(default_factory=list)
    quote: Optional[bool] = None
    tags: List[str] = field(default_factory=list)
    _extra: Dict[str, Any] = field(default_factory=dict)
