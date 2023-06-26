from typing import Any, Dict, List


from dataclasses import dataclass, field

from dbt.contracts.util import (
    AdditionalPropertiesMixin,
)
from dbt.dataclass_schema import dbtClassMixin, ExtensibleDbtClassMixin


@dataclass
class ProjectDependency(AdditionalPropertiesMixin, ExtensibleDbtClassMixin):
    name: str
    _extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProjectDependencies(dbtClassMixin):
    projects: List[ProjectDependency] = field(default_factory=list)
